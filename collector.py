import twitch
import numpy as np
import aiohttp
import datetime
import asyncio
import asyncpg
import contextlib
import pickle
from tenacity import retry, stop_after_attempt
from collections import namedtuple
import hdbscan

import twitch_chat
from util import split_into_even_size, ExpiredSet
import tsne

Change = namedtuple("Change", ["before", "after"])
class ChatterManager:
    def __init__(self, accumulate_duration=60*60*24 / 60, max_accumulate_duration=60*60*24*21, similarity_count=10, session_file_path="chattermanager_session.pkl", min_chatters = 25):
        self.session = aiohttp.ClientSession()
        self.chatters_accumulates = {}
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
    async def update(self, streamer_logins, accumulate_duration):
        chatters_count = {}
        accumulate_duration = accumulate_duration or self.accumulate_duration
        for login in streamer_logins:
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
        similar_streamers_list, tsne_position, clusters = self.streamers_similarities_statistics()
        return chatters_count, similar_streamers_list, tsne_position, clusters
    def chatters_accumulate(self, streamer_login):
        return self.chatters_accumulates[streamer_login].to_list()
    def streamers_similarities_statistics(self, min_chatters=50):
        logins = list(self.chatters_accumulates.keys())
        similarity_list = []
        similarity_matrix = np.zeros((len(logins), len(logins)))
        for i in range(len(logins)):
            for j in range(i+1, len(logins)):
                ca1 = self.chatters_accumulates[logins[i]]
                ca2 = self.chatters_accumulates[logins[j]]
                n = len(ca1.intersection(ca2))
                similarity = n / (ca1.length() + ca2.length() - n) if ca1.length() and ca2.length() else 0
                similarity_matrix[i, j] = similarity
                similarity_matrix[j, i] = similarity
                #if ca1.length() < min_chatters or ca2.length() < min_chatters:
                #   continue
        for i in range(len(logins)):
            similarity_matrix[i, i] = np.inf
        similar_streamers_list = [(logins[i], [(logins[j], similarity_matrix[i, j]) for j in np.argpartition(-similarity_matrix[i], self.similarity_count)[1:self.similarity_count+1]]) for i in range(len(logins))]
        distance_matrix = 1/similarity_matrix
        pos = tsne.umap(distance_matrix)
        cluster = hdbscan.HDBSCAN()
        cluster.fit(pos)
        cluster = [(login, cluster.labels_[i], cluster.probabilities_[i]) for i, login in enumerate(logins)]
        pos = tsne.grid(pos)
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


class Collector:
    def __init__(self, client_args, db_args, language):
        self.client_args = client_args
        self.db_args = db_args
        self.stream_change_cache = {}
        self.language = language
    async def __aenter__(self):
        self.dbconn = await asyncpg.connect(**self.db_args)
        self.twitch_client = twitch.Helix(**self.client_args)
        self.chatter_manager = ChatterManager()
        #for login in self.streamer_logins:
            #self.tiwtch_chats[login] = await twitch_chat.Client.connect(login)
        try:
            self.game_id_set = set(i["id"] for i in await self.dbconn.fetch("SELECT id FROM games"))
            self.streamer_id_to_login = {i["id"]: i["login"] for i in await self.dbconn.fetch("SELECT id, login FROM streamers")}
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
    async def run(self, interval_seconds):
        last = datetime.datetime.now()
        interval = datetime.timedelta(seconds=interval_seconds)
        time_elapsed = interval_seconds
        while True:
            print("time elapsed:", time_elapsed)
            await self._collect(time_elapsed)
            self.chatter_manager.save_session()
            now = datetime.datetime.now()
            time_elapsed = (now - last).total_seconds()
            #if now <= last + interval:
            #    await asyncio.sleep((last + interval-now).total_seconds())
            #time_elapsed = (now - last).total_seconds()
            last = now
    async def _collect(self, time_elapsed):
        print("top 100 streamers update")
        await asyncio.sleep(1)
        await self._top100_streamers_update()
        await asyncio.sleep(1)
        print("fetch streams")
        streams = await self._streams()
        print("fetch games")
        await asyncio.sleep(1)
        await self._update_games(streams)
        now = datetime.datetime.now()
        logined_streamers = [self.streamer_id_to_login[stream["user_id"]] for stream in streams]
        print("fetch chatters info")
        chatter_count, similar_streamers_list, streamer_tsne_grid_pos, streamer_clusters = await self.chatter_manager.update(logined_streamers, accumulate_duration=72*7*time_elapsed)
        chatter_count = {self.streamer_login_to_id[k]: v for k,v in chatter_count.items()}
        similar_streamers_list = [(self.streamer_login_to_id[login1], [(self.streamer_login_to_id[login2], ratio) for (login2, ratio) in similarities]) for (login1, similarities) in similar_streamers_list]
        streamer_tsne_grid_pos = [(self.streamer_login_to_id[login], pos) for (login, pos) in streamer_tsne_grid_pos]
        streamer_clusters = [(self.streamer_login_to_id[login], cluster, probability) for (login, cluster, probability) in streamer_clusters]
        print(f"collecting streams: {len([i for i in streams if i is not None])}")
        if similar_streamers_list:
            async with self.lock:
                async with self.dbconn.transaction():
                    await self.dbconn.execute(f""" TRUNCATE streamer_similarities; """)
                    for (subject, similarities) in similar_streamers_list:
                        for (object, ratio) in similarities:
                            await self.dbconn.execute(f"""
                                INSERT INTO streamer_similarities
                                (subject, object, ratio) VALUES ($1, $2, $3)
                                ON CONFLICT (subject, object) DO
                                UPDATE SET ratio = EXCLUDED.ratio; """
                                , subject, object, ratio)
        if streamer_tsne_grid_pos:
            async with self.lock:
                async with self.dbconn.transaction():
                    await self.dbconn.execute(f""" TRUNCATE streamer_tsne_pos; """)
                    for (streamer_id, (x, y)) in streamer_tsne_grid_pos:
                        await self.dbconn.execute(f"""
                            INSERT INTO streamer_tsne_pos
                            (streamer_id, x, y) VALUES ($1, $2, $3); """
                            , streamer_id, x, y)
        if streamer_clusters:
            async with self.lock:
                async with self.dbconn.transaction():
                    await self.dbconn.execute(f""" TRUNCATE streamer_clusters; """)
                    for (streamer_id, cluster, probability) in streamer_clusters:
                        await self.dbconn.execute(f"""
                            INSERT INTO streamer_clusters
                            (streamer_id, cluster, probability) VALUES ($1, $2, $3); """
                            , streamer_id, cluster, probability)
        for stream in streams:
            if stream:
                if chatter_count.get(stream["user_id"]) is None:
                    continue
                stream["chatter_count"] = chatter_count[stream["user_id"]]
                stream["follower_count"] = await self.follower_count(stream["user_id"])
                async with self.lock:
                    if not self.streamer_is_streaming.get(stream["user_id"]):
                        await self.dbconn.execute("""UPDATE streamers SET is_streaming = TRUE WHERE id = $1""", stream["user_id"])
                        self.streamer_is_streaming[stream["user_id"]] = True
                    cache = self.stream_change_cache.get(stream["user_id"])
                    if cache is None or any(cache[key] != stream[key] for key in ["game_id", "language", "title", "started_at"]):
                        await self.dbconn.execute(f"""
                            INSERT INTO streamer_stream_metadata_changes
                            (streamer_id, time, language, game_id, title, started_at)
                            VALUES ($1, $2, $3, $4, $5, $6) 
                            """, stream["user_id"], now, stream["language"], stream["game_id"], stream["title"], stream["started_at"])
                    if cache is None or cache["viewer_count"] != stream["viewer_count"]:
                        await self.dbconn.execute(f"""
                            INSERT INTO streamer_viewer_count_changes
                            (streamer_id, time, viewer_count)
                            VALUES ($1, $2, $3)
                            """, stream["user_id"], now, stream["viewer_count"])
                    if cache is None or cache["chatter_count"] != stream["chatter_count"]:
                        await self.dbconn.execute(f"""
                            INSERT INTO streamer_chatter_count_changes
                            (streamer_id, time, chatter_count)
                            VALUES ($1, $2, $3)
                            """, stream["user_id"], now, stream["chatter_count"])
                    if cache is None or cache["follower_count"] != stream["follower_count"]:
                        await self.dbconn.execute(f"""
                            INSERT INTO streamer_follower_count_changes
                            (streamer_id, time, follower_count)
                            VALUES ($1, $2, $3)
                            """, stream["user_id"], now, stream["follower_count"])
                    self.stream_change_cache[stream["user_id"]] = stream
        streaming_streamers = set(stream["user_id"] for stream in streams)
        last_streaming_streamers = set(streamer_id for streamer_id, is_streaming in self.streamer_is_streaming.items() if is_streaming)
        for i in (last_streaming_streamers - streaming_streamers):
            self.streamer_is_streaming[i] = False
            async with self.lock:
                await self.dbconn.execute("""UPDATE streamers SET is_streaming = FALSE WHERE id = $1""", i)
            self.stream_change_cache[i] = None
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
            stream["started_at"] = datetime.datetime.fromisoformat(stream["started_at"][:-1])
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
            async with self.lock:
                await self.dbconn.execute("""
                INSERT INTO streamers
                    (id, name, login, profile_image_url, offline_image_url, broadcaster_type, description, type)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (id) DO
                    UPDATE SET name = EXCLUDED.name, profile_image_url = EXCLUDED.profile_image_url, offline_image_url = EXCLUDED.offline_image_url,
                    broadcaster_type = EXCLUDED.broadcaster_type, description = EXCLUDED.description, type = EXCLUDED.type; """
                , int(user["id"]), user["display_name"], user["login"], user["profile_image_url"], user["offline_image_url"], user["broadcaster_type"], user["description"], user["type"])
            self.streamer_id_to_login[int(user["id"])] = user["login"]
            self.streamer_login_to_id[user["login"]] = int(user["id"])
            self.streamer_ids.add(int(user["id"]))
            self.streamer_logins.add(user["login"])
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
    async with Collector(client_args, db_args, "ko") as collector:
        print(op)
        if op == "init":
            await collector.init()
        elif op == "drop":
            await collector.drop()
        elif op == "run":
            await collector.run(300)
        elif op == "test":
            await test_chatter_manager()
            exit()
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
