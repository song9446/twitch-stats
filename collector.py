import threading
import twitch
import numpy as np
import aiohttp
import datetime
import time
import asyncio
import asyncpg
import contextlib
import pickle
from tenacity import retry, stop_after_attempt
from collections import namedtuple
import queue
import hdbscan

import twitch_chat
from util import split_into_even_size, ExpiredSet, MergedStream
import tsne
import telegram_bot

Change = namedtuple("Change", ["before", "after"])
class ChatterManager:
    def __init__(self, accumulate_duration=60*60*24 / 60, max_accumulate_duration=60*60*24*21, similarity_count=10, session_file_path="chattermanager_session.pkl", min_chatters = 25):
        self.session = aiohttp.ClientSession()
        self.chatters_accumulates = {}
        self.chatters_accumulates_lock = threading.Lock()
        #self.updated = {}
        self.accumulate_duration = accumulate_duration
        self.similarity_count = similarity_count
        self.session_file_path = session_file_path
        self.max_accumulate_duration = max_accumulate_duration
        self.min_chatters = min_chatters
        self.load_session()
    def load_session(self):
        try:
            with open(self.session_file_path, "rb") as f:
                self.chatters_accumulates = pickle.load(f)
        except Exception as e:
            print(e)
    def save_session(self):
        with open(self.session_file_path, "wb") as f:
            pickle.dump(self.chatters_accumulates, f, protocol=-1)
    async def update(self, streamer_logins, streamer_logins_all, accumulate_duration):
        with self.chatters_accumulates_lock:
            chatters_count = {}
            accumulate_duration = accumulate_duration or self.accumulate_duration
            for i, login in enumerate(streamer_logins):
                chatters = await self._fetch_chatters(login)
                if chatters is None:
                    continue
                chatters_count[login] = len(chatters)
                self.chatters_accumulates.setdefault(login, ExpiredSet(self.accumulate_duration))
                chatters_accumulates = self.chatters_accumulates[login]
                chatters_accumulates.maintain()
                for chatter in chatters:
                    chatters_accumulates.add(chatter, accumulate_duration, self.max_accumulate_duration)
            for login, chatters_accumulates in list(self.chatters_accumulates.items()):
                chatters_accumulates.maintain()
                if chatters_accumulates.length() < self.min_chatters: 
                    self.chatters_accumulates.pop(login)
            return chatters_count
        #similar_streamers_list, tsne_position, clusters = self.streamers_similarities_statistics(streamer_logins_all)
        #return chatters_count, similar_streamers_list, tsne_position, clusters
    def chatters_accumulate(self, streamer_login):
        return self.chatters_accumulates[streamer_login].to_list()
    def streamers_similarities_statistics(self, logins, min_chatters=50):
        #logins = list(self.chatters_accumulates.keys())
        with self.chatters_accumulates_lock:
            logins = [i for i in logins if i in self.chatters_accumulates]
        similarity_list = []
        similarity_matrix = np.zeros((len(logins), len(logins)))
        intersections_matrix = np.zeros((len(logins), len(logins)))
        for i in range(len(logins)):
            if logins[i] not in self.chatters_accumulates:
                continue
            for j in range(i+1, len(logins)):
                if logins[j] not in self.chatters_accumulates:
                    continue
                ca1 = self.chatters_accumulates[logins[i]]
                ca2 = self.chatters_accumulates[logins[j]]
                n = len(ca1.intersection(ca2))
                similarity = n / (ca1.length() + ca2.length() - n) if ca1.length() and ca2.length() else 0
                similarity_matrix[i, j] = similarity
                similarity_matrix[j, i] = similarity
                intersections_matrix[i, j] = n
                intersections_matrix[j, i] = n
                #if ca1.length() < min_chatters or ca2.length() < min_chatters:
                #   continue
        for i in range(len(logins)):
            similarity_matrix[i, i] = np.inf
        similar_streamers_list = [(logins[i], [(logins[j], similarity_matrix[i, j]) for j in np.argpartition(-similarity_matrix[i], self.similarity_count)[1:self.similarity_count+1]]) for i in range(len(logins))]
        distance_matrix = 1/similarity_matrix
        pos = tsne.umap(distance_matrix)
        cluster = hdbscan.HDBSCAN()
        cluster.fit(pos)
        indexes_by_cluster = [[] for _ in range(np.max(cluster.labels_)+1)]
        for i, _ in enumerate(logins):
            if cluster.labels_[i] >= 0: 
                indexes_by_cluster[cluster.labels_[i]].append(i)
        ratio_by_index = [0] * len(logins)
        for _, indexes in enumerate(indexes_by_cluster):
            ratios = [sum(intersections_matrix[i, j] for j in indexes if i != j) for i in indexes]
            ratios_sum = sum(ratios) + 0.00000001
            ratios = [ratio/ratios_sum for ratio in ratios]
            for ratio, index in zip(ratios, indexes):
                ratio_by_index[index] = ratio
        cluster = [(login, cluster.labels_[i], ratio_by_index[i]) for i, login in enumerate(logins)]
        pos = tsne.grid(pos, 2)
        pos = [(login, pos[i]) for i, login in enumerate(logins)]
        """
        #pos = tsne.tsne_grid(similarity_matrix, 1000)
        pos = tsne.umap_grid(distance_matrix)
        pos = [(login, pos[i]) for i, login in enumerate(logins)]
        cluster = hdbscan.HDBSCAN(
                metric='precomputed', 
                cluster_selection_method='eom',#'leaf'
                cluster_selection_epsilon=10,
                min_samples=1,
                )
        cluster.fit(distance_matrix)
        cluster = [(login, cluster.labels_[i], cluster.probabilities_[i]) for i, login in enumerate(logins)]
        """
        return similar_streamers_list, pos, cluster
    @retry(stop=stop_after_attempt(100))
    async def _fetch_chatters(self, channel):
        async with self.session.get(f"https://tmi.twitch.tv/group/user/{channel}/chatters") as resp:
            json = await resp.json()
            if len(json) == 0:
                return None
            viewers = set(json["chatters"]["viewers"])
            return viewers

async def test_chatter_manager():
    cm = ChatterManager()
    print(len(cm.chatters_accumulates))
    print(cm.streamers_similarities_statistics()[2])


class ChatManager:
    def __init__(self):
        self.chat_counts = {}
        self.queue = queue.Queue()
        self.ADD = 0
        self.REMOVE = 1
        self.EXTEND = 2
        self.error = None
        def run(self):
            async def _run():
                merged_stream = MergedStream()
                twitch_chats = {}
                async def dequeue():
                    while True:
                        try:
                            op, login = self.queue.get_nowait()
                            if op == self.EXTEND:
                                logins = [l for l in login if l not in twitch_chats]
                                res = await asyncio.gather(*[twitch_chat.Client.connect(l) for l in logins])
                                for i, l in enumerate(logins):
                                    twitch_chats[l] = res[i]
                                    merged_stream.append(res[i])
                            elif op == self.ADD:
                                if login not in twitch_chats:
                                    twitch_chats[login] = await twitch_chat.Client.connect(login)
                                    merged_stream.append(twitch_chats[login])
                            elif op == self.REMOVE:
                                if login in twitch_chats:
                                    await twitch_chats.pop(login).close()
                                if login in self.chat_counts:
                                    self.chat_counts.pop(login)
                        except queue.Empty:
                            break
                try:
                    while True:
                        await dequeue()
                        async for msg in merged_stream:
                            await dequeue()
                            if msg.channel in twitch_chats:
                                self.chat_counts.setdefault(msg.channel, 0)
                                self.chat_counts[msg.channel] += 1
                        time.sleep(1)
                except Exception as e:
                    self.error = e
                    raise e
            asyncio.run(_run())
        t = threading.Thread(target=run, args=(self,))
        t.daemon = True
        t.start()
    def add(self, login):
        self.queue.put_nowait((self.ADD, login))
    def extend(self, logins):
        self.queue.put_nowait((self.EXTEND, logins))
    def remove(self, login):
        self.queue.put_nowait((self.REMOVE, login))
    def drain_chat_counts(self):
        chat_counts = self.chat_counts
        self.chat_counts = {}
        return chat_counts

async def test_chat_manager():
    cm = ChatManager()
    cm.extend(["saddummy", "handongsuk"])
    time.sleep(10)
    print(cm.drain_chat_counts())
    time.sleep(10)
    print(cm.drain_chat_counts())
    time.sleep(10)
    print(cm.drain_chat_counts())
    time.sleep(10)
    print(cm.drain_chat_counts())
    time.sleep(10)
    print(cm.drain_chat_counts())
    time.sleep(10)
    print(cm.drain_chat_counts())


class Collector:
    def __init__(self, client_args, db_args, language):
        self.client_args = client_args
        self.db_args = db_args
        self.stream_change_cache = {}
        self.language = language
        self.error = None
    async def __aenter__(self):
        self.dbconn = await asyncpg.connect(**self.db_args)
        self.twitch_client = twitch.Helix(**self.client_args)
        self.chatter_manager = ChatterManager()
        self.chat_manager = ChatManager()
        try:
            self.game_id_set = set(i["id"] for i in await self.dbconn.fetch("SELECT id FROM games"))
            self.streamer_id_to_login = {i["id"]: i["login"] for i in await self.dbconn.fetch("SELECT id, login FROM streamers")}
            self.streamer_id_to_average_viewer_count = {i["id"]: i["average_viewer_count"] for i in await self.dbconn.fetch("SELECT id, average_viewer_count FROM streamers")}
            self.streamer_login_to_id = {v: k for k,v in self.streamer_id_to_login.items()}
            self.streamer_ids = set(self.streamer_id_to_login.keys())
            self.streamer_logins = set(self.streamer_id_to_login.values())
            self.streamer_is_streaming = {i["id"]: True for i in await self.dbconn.fetch("SELECT id FROM streamers WHERE is_streaming = TRUE")}
        except Exception as e:
            print(e)
            self.game_id_set = set()
            self.streamer_id_to_login = {}
            self.streamer_login_to_id = {}
            self.streamer_ids = set()
            self.streamer_logins = set()
        self.twitch_chats = {}
        #for login in self.streamer_logins:
            #self.tiwtch_chats[login] = await twitch_chat.Client.connect(login)
        self.lock = asyncio.Lock()
        return self
    async def __aexit__(self, exc_type, exc, tb):
        await self.dbconn.close()
    async def init(self):
        pass
    async def drop(self):
        pass
    async def add_streamer(self, login):
        user = self.twitch_client.user(login).data
        async with self.lock:
            await self.dbconn.execute("""
            INSERT INTO streamers
                (id, name, login, profile_image_url, offline_image_url, broadcaster_type, description, type)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
            """, int(user["id"]), user["display_name"], user["login"], user["profile_image_url"], user["offline_image_url"], user["broadcaster_type"], user["description"], user["type"])
        self.streamer_id_to_login[int(user["id"])] = login
        self.streamer_login_to_id[login] = int(user["id"])
        self.streamer_ids.add(int(user["id"]))
        self.streamer_logins.add(login)
        self.streamer_id_to_average_viewer_count[int(user["id"])] = 0
    async def run(self, interval_seconds):
        def _run_similarities_statistics(self):
            async def run():
                try:
                    dbconn = await asyncpg.connect(**self.db_args)
                    print("similarities_statistics_collect_start")
                    last = datetime.datetime.now()
                    while True:
                        res = self._collect_similarities_statistics()
                        await Collector._update_similarities_statistics(dbconn, *res)
                        now = datetime.datetime.now()
                        time_elapsed = (now - last).total_seconds()
                        last = now
                except Exception as e:
                    self.error = e
                    raise e
            asyncio.run(run())
        t = threading.Thread(target=_run_similarities_statistics, args=(self,))
        t.daemon = True
        t.start()
        last = datetime.datetime.now()
        interval = datetime.timedelta(seconds=interval_seconds)
        time_elapsed = interval_seconds
        while True:
            if self.chat_manager.error:
                raise self.chat_manager.error
            if self.error:
                raise self.error
            self.time_elapsed = time_elapsed
            await self._collect_and_update_counts(time_elapsed)
            now = datetime.datetime.now()
            time_elapsed = (now - last).total_seconds()
            #if now <= last + interval:
            #    await asyncio.sleep((last + interval-now).total_seconds())
            #time_elapsed = (now - last).total_seconds()
            last = now
    async def _update_similarities_statistics(dbconn, similar_streamers_list, streamer_tsne_grid_pos, streamer_clusters):
        if similar_streamers_list:
            async with dbconn.transaction():
                await dbconn.execute(f""" TRUNCATE streamer_similarities; """)
                for (subject, similarities) in similar_streamers_list:
                    for (object, ratio) in similarities:
                        await dbconn.execute(f"""
                            INSERT INTO streamer_similarities
                            (subject, object, ratio) VALUES ($1, $2, $3)
                            ON CONFLICT (subject, object) DO
                            UPDATE SET ratio = EXCLUDED.ratio; """
                            , subject, object, ratio)
        if streamer_tsne_grid_pos:
            async with dbconn.transaction():
                await dbconn.execute(f""" TRUNCATE streamer_tsne_pos; """)
                for (streamer_id, (x, y)) in streamer_tsne_grid_pos:
                    await dbconn.execute(f"""
                        INSERT INTO streamer_tsne_pos
                        (streamer_id, x, y) VALUES ($1, $2, $3); """
                        , streamer_id, x, y)
        if streamer_clusters:
            async with dbconn.transaction():
                await dbconn.execute(f""" TRUNCATE streamer_clusters; """)
                for (streamer_id, cluster, probability) in streamer_clusters:
                    await dbconn.execute(f"""
                        INSERT INTO streamer_clusters
                        (streamer_id, cluster, probability) VALUES ($1, $2, $3); """
                        , streamer_id, cluster, probability)

    def _collect_similarities_statistics(self):
        similar_streamers_list, streamer_tsne_grid_pos, streamer_clusters = self.chatter_manager.streamers_similarities_statistics(self.streamer_logins)
        similar_streamers_list = [(self.streamer_login_to_id[login1], [(self.streamer_login_to_id[login2], ratio) for (login2, ratio) in similarities if login2 in self.streamer_login_to_id]) for (login1, similarities) in similar_streamers_list if login1 in self.streamer_login_to_id]
        streamer_tsne_grid_pos = [(self.streamer_login_to_id[login], pos) for (login, pos) in streamer_tsne_grid_pos if login in self.streamer_login_to_id]
        streamer_clusters = [(self.streamer_login_to_id[login], cluster, probability) for (login, cluster, probability) in streamer_clusters if login in self.streamer_login_to_id]
        return similar_streamers_list, streamer_tsne_grid_pos, streamer_clusters
    async def _collect_and_update_counts(self, time_elapsed):
        await asyncio.sleep(1)
        await self._top100_streamers_update()
        await asyncio.sleep(1)
        streams = await self._streams()
        await asyncio.sleep(1)
        await self._update_games(streams)
        now = datetime.datetime.now()
        logined_streamers = [self.streamer_id_to_login[stream["user_id"]] for stream in streams]
        self.chat_manager.extend(logined_streamers)
        chat_count = self.chat_manager.drain_chat_counts()
        chat_count = {self.streamer_login_to_id[k]: v for k,v in chat_count.items()}
        #similar_streamers_list, tsne_position, clusters = self.streamers_similarities_statistics(streamer_logins_all)
        chatter_count = await self.chatter_manager.update(logined_streamers, self.streamer_logins, accumulate_duration=72*7*time_elapsed)
        chatter_count = {self.streamer_login_to_id[k]: v for k,v in chatter_count.items()}
        for i, stream in enumerate(streams):
            if stream:
                if chatter_count.get(stream["user_id"]) is None:
                    continue
                stream["chatter_count"] = chatter_count[stream["user_id"]]
                stream["chat_count"] = chat_count.get(stream["user_id"], 0)
                #stream["follower_count"] = await self.follower_count(stream["user_id"])
                stream["follower_count"] = 0
                async with self.lock:
                    if not self.streamer_is_streaming.get(stream["user_id"]):
                        await self.dbconn.execute("""UPDATE streamers SET is_streaming = TRUE WHERE id = $1""", stream["user_id"])
                        self.streamer_is_streaming[stream["user_id"]] = True
                    cache = self.stream_change_cache.get(stream["user_id"])
                    if cache is None or any(cache[key] != stream[key] for key in ["game_id", "language", "title", "started_at"]):
                        await self.dbconn.execute(f"""
                            INSERT INTO stream_metadata_changes
                            (streamer_id, time, language, game_id, title, started_at)
                            VALUES ($1, $2, $3, $4, $5, $6) 
                            """, stream["user_id"], now, stream["language"], stream["game_id"], stream["title"], stream["started_at"])
                    await self.dbconn.execute(f"""
                        INSERT INTO stream_changes
                        (streamer_id, time, viewer_count, chatter_count, chatting_speed, follower_count)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        """, stream["user_id"], now, stream["viewer_count"], stream["chatter_count"], stream["chat_count"]/time_elapsed, stream["follower_count"])
                    if stream["user_id"] not in self.streamer_id_to_average_viewer_count or self.streamer_id_to_average_viewer_count[stream["user_id"]] == 0: 
                        self.streamer_id_to_average_viewer_count[stream["user_id"]] = stream["viewer_count"]
                    else:
                        self.streamer_id_to_average_viewer_count[stream["user_id"]] = self.streamer_id_to_average_viewer_count[stream["user_id"]]*0.99 + stream["viewer_count"] * 0.01
                    await self.dbconn.execute(f"""
                        UPDATE streamers SET average_viewer_count = $1 WHERE id = $2
                        """, self.streamer_id_to_average_viewer_count[stream["user_id"]], stream["user_id"])
                    self.stream_change_cache[stream["user_id"]] = stream
        streaming_streamers = set(stream["user_id"] for stream in streams)
        last_streaming_streamers = set(streamer_id for streamer_id, is_streaming in self.streamer_is_streaming.items() if is_streaming)
        for i in (last_streaming_streamers - streaming_streamers):
            self.chat_manager.remove(self.streamer_id_to_login[i])
            self.streamer_is_streaming.pop(i, None)
            async with self.lock:
                await self.dbconn.execute("""UPDATE streamers SET is_streaming = FALSE WHERE id = $1""", i)
                if self.stream_change_cache.get(i):
                    await self.dbconn.execute("""INSERT INTO stream_ranges (streamer_id, range) VALUES ($1, $2)""", i, (self.stream_change_cache[i]["started_at"], now))
            self.stream_change_cache.pop(i, None)
    @retry(stop=stop_after_attempt(100))
    async def _streams(self):
        streamer_id_chunks = split_into_even_size(list(self.streamer_ids), 100)
        streams = []
        for streamer_ids in streamer_id_chunks:
            try:
                streams.extend(self.twitch_client.streams(user_id=streamer_ids))
            except twitch.helix.StreamNotFound as e:
                pass
        streams = [i.data for i in streams]
        for stream in streams:
            stream["id"] = int(stream["id"])
            stream["game_id"] = (stream["game_id"] and int(stream["game_id"])) or None
            stream["viewer_count"] = int(stream["viewer_count"])
            stream["user_id"] = int(stream["user_id"])
            stream["started_at"] = datetime.datetime.fromisoformat(stream["started_at"][:-1] + "+00:00")
        return streams
    @retry(stop=stop_after_attempt(100))
    async def _update_games(self, streams):
        new_game_id_set = set()
        for stream in streams:
            if stream["game_id"] and (int(stream["game_id"]) not in self.game_id_set):
                new_game_id_set.add(int(stream["game_id"]))
        new_game_ids = list(new_game_id_set)
        game_id_chunks = split_into_even_size(new_game_ids, 100)
        game_chunks = (self.twitch_client.games(id=game_ids) for game_ids in game_id_chunks)
        games = [game.data for games in game_chunks for game in games]
        for game in games:
            await self.dbconn.execute("""
                INSERT INTO games (id, name, box_art_url) VALUES ($1, $2, $3)
                """, int(game["id"]), game["name"], game["box_art_url"])
        self.game_id_set = self.game_id_set.union(new_game_id_set)
    @retry(stop=stop_after_attempt(100))
    async def follower_count(self, user_id):
        return twitch.helix.Follows(self.twitch_client.api, follow_type="followers", to_id=str(user_id)).total
    @retry(stop=stop_after_attempt(100))
    async def _top100_streamers_update(self, min_viewers=100):
        streams = self.twitch_client.streams(language=self.language, first=100)
        user_ids = [int(stream.data["user_id"]) for stream in streams if int(stream.data["viewer_count"]) >= min_viewers]
        users = self.twitch_client.users(user_ids)
        users = [user.data for user in users]
        #user = self.twitch_client.user(login).data
        for user in users:
            userid = int(user["id"])
            async with self.lock:
                await self.dbconn.execute("""
                INSERT INTO streamers
                    (id, name, login, profile_image_url, offline_image_url, broadcaster_type, description, type)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (id) DO
                    UPDATE SET name = EXCLUDED.name, profile_image_url = EXCLUDED.profile_image_url, offline_image_url = EXCLUDED.offline_image_url,
                    broadcaster_type = EXCLUDED.broadcaster_type, description = EXCLUDED.description, type = EXCLUDED.type; """
                , int(user["id"]), user["display_name"], user["login"], user["profile_image_url"], user["offline_image_url"], user["broadcaster_type"], user["description"], user["type"])
            if userid in self.streamer_id_to_login:
                if self.streamer_id_to_login[userid] in self.streamer_login_to_id:
                    self.streamer_login_to_id.pop(self.streamer_id_to_login[userid])
                    self.streamer_logins.remove(self.streamer_id_to_login[userid])
            self.streamer_id_to_login[userid] = user["login"]
            self.streamer_login_to_id[user["login"]] = userid
            self.streamer_ids.add(userid)
            self.streamer_logins.add(user["login"])
            if userid not in self.streamer_id_to_average_viewer_count:
                self.streamer_id_to_average_viewer_count[userid] = 0
#asyncio.run(init(500, 60))
#asyncio.run(run(500, 60))
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
                async with Collector(client_args, db_args, "ko") as collector:
                    await collector.run(300)
            except Exception as e:
                telegram_bot.send_message("restart collector due to:")
                telegram_bot.send_message(repr(e))
                time.sleep(180)
        telegram_bot.send_message("collector shutdowned unexpecteadly!!")
    elif op == "test":
        await test_chat_manager()
        #await collector._collect(60)
        #await collector._collect_similarities_statistics()
        #await test_chatter_manager()
        #exit()
        #await collector.run(60)

if __name__ == "__main__":
    import sys
    commands = ["init", "run", "drop", "test"]
    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print(f"""usage
    {sys.argv[0]} init
    {sys.argv[0]} run""")
        exit(1)
    asyncio.run(main(sys.argv[1]))
