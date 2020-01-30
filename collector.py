from typing import List
import asyncio
import time
import asyncpg
import pickle
from api import API, Stream, User
from api import twitch

from util import split_into_even_size, ExpiredSet, MergedStream
import telegram_bot
import datetime
from dataclasses import dataclass
from shutil import copyfile
import numpy as np

import hdbscan
import tsne

from multiprocessing.pool import ThreadPool

np.seterr(divide='ignore')

@dataclass
class AdvancedStatisticsManager:
    api: API
    session_file_path: str = "collector_session.pkl"
    accumulate_duration_factor: float = 24*7
    max_accumulate_duration: float = 60*60*24*21
    similarity_count: int = 10
    min_chatters: int = 25
    chatters_accumulates = {}
    def __post_init__(self):
        self.load_session()
    def load_session(self):
        try:
            with open(self.session_file_path, "rb") as f:
                session = pickle.load(f)
                self.chatters_accumulates = session["chatters_accumulates"]
        except Exception as e:
            try:
                with open(self.session_file_path + ".back", "rb") as f:
                    session = pickle.load(f)
                    self.chatters_accumulates = session["chatters_accumulates"]
            except Exception as e:
                print(repr(e))
    def save_session(self):
        copyfile(self.session_file_path, self.session_file_path + ".back")
        with open(self.session_file_path, "wb") as f:
            session = {"chatters_accumulates": self.chatters_accumulates}
            pickle.dump(session, f, protocol=-1)
    def update(self, streams: List[Stream], elapsed_time):
        for s in streams:
            self.chatters_accumulates.setdefault(s.user.id, ExpiredSet(self.max_accumulate_duration))
            chatters_accumulates = self.chatters_accumulates[s.user.id]
            chatters_accumulates.maintain()
            for chatter in s.chatters:
                chatters_accumulates.add(chatter, elapsed_time*self.accumulate_duration_factor)
        for id, chatters_accumulates in list(self.chatters_accumulates.items()):
            chatters_accumulates.maintain()
            if chatters_accumulates.length() < self.min_chatters:
                self.chatters_accumulates.pop(id)
        self.save_session()
    def calculate_statistics(self):
        print("start calculate statistics..", datetime.datetime.now())
        ids = [id for id in self.chatters_accumulates.keys()]
        similarity_matrix = np.zeros((len(ids), len(ids)))
        intersections_matrix = np.zeros((len(ids), len(ids)))
        for i in range(len(ids)):
            if ids[i] not in self.chatters_accumulates:
                continue
            for j in range(i+1, len(ids)):
                if ids[j] not in self.chatters_accumulates:
                    continue
                ca1 = self.chatters_accumulates[ids[i]]
                ca2 = self.chatters_accumulates[ids[j]]
                n = len(ca1.intersection(ca2))
                similarity = n / (ca1.length() + ca2.length() - n) if ca1.length() and ca2.length() else 0
                similarity_matrix[i, j] = similarity
                similarity_matrix[j, i] = similarity
                intersections_matrix[i, j] = n
                intersections_matrix[j, i] = n
        for i in range(len(ids)):
            similarity_matrix[i, i] = np.inf
        similar_streamers_list = [(ids[i], [(ids[j], similarity_matrix[i, j]) for j in np.argpartition(-similarity_matrix[i], self.similarity_count)[1:self.similarity_count+1]]) for i in range(len(ids))]
        distance_matrix = 1/similarity_matrix
        pos = tsne.umap(distance_matrix)
        #pos = tsne.tsne(distance_matrix)
        cluster = hdbscan.HDBSCAN()
        cluster.fit(pos)
        indexes_by_cluster = [[] for _ in range(np.max(cluster.labels_)+1)]
        for i, _ in enumerate(ids):
            if cluster.labels_[i] >= 0: 
                indexes_by_cluster[cluster.labels_[i]].append(i)
        ratio_by_index = [0] * len(ids)
        for _, indexes in enumerate(indexes_by_cluster):
            ratios = [sum(intersections_matrix[i, j] for j in indexes if i != j) for i in indexes]
            ratios_sum = sum(ratios) + 0.00000001
            ratios = [ratio/ratios_sum for ratio in ratios]
            for ratio, index in zip(ratios, indexes):
                ratio_by_index[index] = ratio
        cluster = [(login, cluster.labels_[i], ratio_by_index[i]) for i, login in enumerate(ids)]
        pos = tsne.grid(pos, 3)
        pos = [(id, pos[i]) for i, id in enumerate(ids)]
        print("calculate statistics end", datetime.datetime.now())
        return similar_streamers_list, pos, cluster

class Collector:
    def __init__(self, api: API, db_args):
        self.api = api
        self.db_args = db_args
        self.streaming_streamers = set()
        self.last_streams = {}
        self.average_viewer_counts = {}
        self.advanced_statistics_manager = AdvancedStatisticsManager(api=api)
        self.pool = ThreadPool(1)
    async def run(self, interval_seconds=60, interval_seconds_for_advacned_statistics_calculate=300):
        self.dbconn = await asyncpg.connect(**self.db_args)
        self.streaming_streamers = set(User(**i) for i in await self.dbconn.fetch("SELECT id, name, login, profile_image_url, offline_image_url, broadcaster_type, description, type FROM streamers WHERE is_streaming = TRUE"))
        self.average_viewer_counts = {}
        time_elapsed = interval_seconds
        interval = datetime.timedelta(seconds=interval_seconds)
        interval_for_advacned_statistics_calculate = datetime.timedelta(seconds=interval_seconds_for_advacned_statistics_calculate)
        now = datetime.datetime.now()
        last = now - interval
        last_advanced_statistics_updated = last
        advanced_calcuate_result_future = None
        while True:
            last = now
            await self.collect(time_elapsed)
            if advanced_calcuate_result_future is None:
                advanced_calcuate_result_future = self.pool.apply_async(self.advanced_statistics_manager.calculate_statistics)
            elif advanced_calcuate_result_future.ready() and now - last_advanced_statistics_updated >= interval_for_advacned_statistics_calculate:
                await self.advanced_statistics_db_update(*advanced_calcuate_result_future.get())
                advanced_calcuate_result_future = None
                last_advanced_statistics_updated = now
                print("advanced_statistics_update at", datetime.datetime.now())
            now = datetime.datetime.now()
            time_elapsed = (now - last).total_seconds()
            if time_elapsed < interval_seconds:
                await asyncio.sleep(interval_seconds - time_elapsed)
            now = datetime.datetime.now()
            time_elapsed = (now - last).total_seconds()
            print("time elapsed for collect", time_elapsed)
        pool.close()
        pool.join()
    async def advanced_statistics_db_update(self, similar_streamers_list, streamer_tsne_grid_pos, streamer_clusters):
        if similar_streamers_list:
            async with self.dbconn.transaction():
                records = [(subject, object, ratio) for subject, similarities in similar_streamers_list for object, ratio in similarities]
                await self.dbconn.execute(f""" TRUNCATE streamer_similarities; """)
                await self.dbconn.copy_records_to_table("streamer_similarities",
                        columns=["subject", "object", "ratio"],
                        records=records)
        if streamer_tsne_grid_pos:
            async with self.dbconn.transaction():
                await self.dbconn.execute(f""" TRUNCATE streamer_tsne_pos; """)
                await self.dbconn.copy_records_to_table("streamer_tsne_pos",
                        columns=["streamer_id", "x", "y"],
                        records=[(streamer_id, x, y) for (streamer_id, (x, y)) in streamer_tsne_grid_pos])
        if streamer_clusters:
            async with self.dbconn.transaction():
                await self.dbconn.execute(f""" TRUNCATE streamer_clusters; """)
                await self.dbconn.copy_records_to_table("streamer_clusters",
                        columns=["streamer_id", "cluster", "probability"],
                        records=[(id, c, p) for id, c, p in streamer_clusters])
    async def collect(self, elapsed_seconds):
        now = datetime.datetime.now()
        print("start collect streams..", datetime.datetime.now())
        streams = await self.api.streams()
        print("streams collect end", datetime.datetime.now())
        users = set(s.user for s in streams)
        games = set(s.game for s in streams if s.game)
        await self.dbconn.executemany("""
            INSERT INTO streamers
            (id, name, login, profile_image_url, offline_image_url, broadcaster_type, description, type, is_streaming)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE)
            ON CONFLICT (id) DO
            UPDATE SET name = EXCLUDED.name, profile_image_url = EXCLUDED.profile_image_url, offline_image_url = EXCLUDED.offline_image_url,
            broadcaster_type = EXCLUDED.broadcaster_type, description = EXCLUDED.description, type = EXCLUDED.type, is_streaming = TRUE; """
        ,[(user.id, user.name, user.login, user.profile_image_url, user.offline_image_url, user.broadcaster_type, user.description, user.type) for user in users])
        await self.dbconn.executemany("""
            INSERT INTO games 
            (id, name, box_art_url) 
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO
            UPDATE SET name = EXCLUDED.name, box_art_url = EXCLUDED.box_art_url; """, 
            [(game.id, game.name, game.box_art_url) for game in games])
        streaming_end_users = [s for s in (self.streaming_streamers - users)]
        self.streaming_streamers = users
        await self.dbconn.executemany("""
            UPDATE streamers SET is_streaming = FALSE WHERE id = $1 """, 
            [(u.id,) for u in streaming_end_users])
        await self.dbconn.copy_records_to_table("stream_ranges",
                columns=["streamer_id", "range"],
                records=[(u.id, asyncpg.Range(self.last_streams[u.id].started_at, now)) for u in streaming_end_users if u.id in self.last_streams])
        metadata_changed_streams = [s for s in streams if not s.metadata_eq(self.last_streams.get(s.user.id))]
        await self.dbconn.copy_records_to_table("stream_metadata_changes", 
                columns=["streamer_id", "time", "language", "game_id", "title", "started_at"], 
                records=[(s.user.id, now, s.language, s.game.id if s.game else None, s.title, s.started_at) for s in metadata_changed_streams])
        for s in streams:
            self.average_viewer_counts.setdefault(s.user.id, [0, 0])
            self.average_viewer_counts[s.user.id][0] += s.viewer_count * elapsed_seconds
            self.average_viewer_counts[s.user.id][1] += elapsed_seconds
        await self.dbconn.executemany("""
            UPDATE streamers SET average_viewer_count = average_viewer_count * 0.9 + $1 * 0.1 WHERE id = $2""", 
            [(self.average_viewer_counts[u.id][0]/self.average_viewer_counts[u.id][1], u.id) for u in streaming_end_users if (u.id in self.average_viewer_counts) and self.average_viewer_counts[u.id][1]])
        for u in streaming_end_users: 
            if u.id in self.average_viewer_counts: 
                self.average_viewer_counts.pop(u.id)
        await self.dbconn.copy_records_to_table("stream_changes", 
                columns=["streamer_id", "time", "viewer_count", "chatter_count", "chatting_speed", "follower_count"],
                records=[(s.user.id, now, s.viewer_count, len(s.chatters), len(s.chattings)/elapsed_seconds, 0) for s in streams])
        self.last_streams = {s.user.id: s for s in streams}
        print("collect end", datetime.datetime.now())
        self.advanced_statistics_manager.update(streams, elapsed_seconds)
        print("advanced statistics update end", datetime.datetime.now())


async def main(op):
    client_args = dict(client_id = "6zqny3p0ft2js766jptev3mvp0ay51",
                   bearer_token="pcjch55ezhaulaptylu85iq2ni4x6t")
    db_args = dict(user='postgres',
            password='Thelifeisonlyonce',
            database='twitch_stats',
            host='133.130.124.159',
            port=5432)
    if op == "run":
        print(op)
        telegram_bot.send_message("start tsu.gg twitch stream statistics collector")
        while True:
            try:
                api = twitch.API(client_args)
                collector = Collector(api, db_args)
                await collector.run(60, 60)
            except Exception as e:
                raise e
                telegram_bot.send_message("restart collector due to:")
                telegram_bot.send_message(repr(e))
                time.sleep(180)
        telegram_bot.send_message("collector shutdowned unexpecteadly!!")
    elif op == "test":
        pass
        #await collector._collect(60)
        #await collector._collect_similarities_statistics()
        #await test_chatter_manager()
        #exit()
        #await collector.run(60)

if __name__ == "__main__":
    import sys
    commands = ["run", "test"]
    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print(f"""usage
    {sys.argv[0]} init
    {sys.argv[0]} run""")
        exit(1)
    asyncio.run(main(sys.argv[1]))
