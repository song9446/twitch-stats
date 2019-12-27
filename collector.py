import twitch
import aiohttp
import datetime
import asyncio
import asyncpg
import contextlib
from collections import namedtuple

import twitch_chat
from util import split_into_even_size, ExpiredSet

Change = namedtuple("Change", ["before", "after"])

class ChatterManager:
    def __init__(self):
        self.session = aiohttp.ClientSession()
        self.chatters = {}
        self.chatters_accumulate = {}
        #self.updated = {}
        self.migrations = {}
    async def update(self, streamer_logins, last_session_file=".chatter_manager_session"):
        new_chatters = {}
        migrations_for_user = {}
        for login in streamer_logins:
            self.chatters.setdefault(login, set())
            new_chatters = await self._fetch_chatters(login)
            last_chatters = self.chatters.get(login)
            comming = new_chatters - last_chatters
            leaving = last_chatters - new_chatters
            for user in leaving:
                migrations_for_user.setdefault(user, Change([], []))
                migrations_for_user[user].before.append(login)
            for user in comming:
                migrations_for_user.setdefault(user, Change([], []))
                migrations_for_user[user].after.append(login)
            self.chatters[login] = new_chatters
            self.chatters_accumulate.setdefault(login, ExpiredSet())
            chatters_accumulate = self.chatters_accumulate[login]
            chatters_accumulate.maintain()
            for new_chatter in new_chatters:
                chatters_accumulate.add(new_chatter)

            #if comming or leaving:
            #    self.updated[login] = True
            #else:
            #   #self.updated[login] = False
        migrations = {}
        for user, change in migrations_for_user.items():
            for before in change.before:
                for after in change.after:
                    key = (before, after)
                    migrations.setdefault(key, 0)
                    migrations[key] += 1
        self.migrations = migrations
        return {k: len(v) for k, v in self.chatters.items()}, self.migrations
    def chatters_accumulate(self, streamer_login):
        return self.chatters_accumulate[streamer_login].to_list()
    def streamers_similarities(self):
        pass
    async def _fetch_chatters(self, channel):
        async with self.session.get(f"https://tmi.twitch.tv/group/user/{channel}/chatters") as resp:
            return set((await resp.json())["chatters"]["viewers"])

async def test_chatter_manager():
    cm = ChatterManager()
    print(await cm.update(["zilioner", "wltn4765", "flurry1989", "velvet_7"]))


class Collector:
    def __init__(self, client_id, db_args):
        self.client_id = client_id
        self.db_args = db_args
        self.stream_change_cache = {}
    async def __aenter__(self):
        self.dbconn = await asyncpg.connect(**self.db_args)
        self.twitch_client = twitch.Helix(client_id = self.client_id, use_cache = False)
        self.streamer_id_to_login = {i["id"]: i["login"] for i in await self.dbconn.fetch("SELECT id, login FROM streamers")}
        self.streamer_login_to_id = {v: k for k,v in self.streamer_id_to_login.items()}
        self.streamer_ids = set(self.streamer_id_to_login.keys())
        self.streamer_logins = set(self.streamer_id_to_login.values())
        self.chatter_manager = ChatterManager()
        #for login in self.streamer_logins:
            #self.tiwtch_chats[login] = await twitch_chat.Client.connect(login)
        try:
            self.game_id_set = set(i["id"] for i in await self.dbconn.fetch("SELECT id FROM games"))
        except Exception as e:
            print(e)
            self.game_id_set = set()
        self.lock = asyncio.Lock()
        return self
    async def __aexit__(self, exc_type, exc, tb):
        await self.dbconn.close()
    async def init(self):
        try:
            await self.drop()
        except Exception as e:
            print(e)
        async with self.lock:
            await self.dbconn.execute("""
            CREATE TABLE streamers (
                id BIGINT PRIMARY KEY,
                name TEXT NOT NULL,
                login TEXT NOT NULL,
                profile_image_url TEXT,
                offline_image_url TEXT,
                broadcaster_type TEXT,
                description TEXT,
                type TEXT,
                is_streaming BOOLEAN
            );
            CREATE TABLE games (
                id BIGINT PRIMARY KEY,
                name TEXT,
                box_art_url TEXT
            );
            CREATE TABLE streams (
                id BIGINT PRIMARY KEY,
                streamer_id BIGINT REFERENCES streamers (id),
                started_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
            CREATE TABLE stream_changes (
                stream_id BIGINT REFERENCES streams (id),
                viewer_count INTEGER,
                chatter_count INTEGER,
                game_id BIGINT REFERENCES games (id),
                language CHAR(2),
                title TEXT,
                time TIMESTAMP NOT NULL DEFAULT NOW(),
                PRIMARY KEY (stream_id, time)
            );
            CREATE TABLE chatter_migrations (
                before BIGINT REFERENCES streamers (id),
                after BIGINT REFERENCES streamers (id),
                count INTEGER NOT NULL DEFAULT 0,
                time TIMESTAMP NOT NULL DEFAULT NOW(),
                PRIMARY KEY (before, after, time)
            );
            """)
    async def drop(self):
        async with self.lock:
            await self.dbconn.execute("""
            DROP TABLE chatter_migrations;
            DROP TABLE stream_changes;
            DROP TABLE streams;
            DROP TABLE games;
            DROP TABLE streamers;
            """)
    async def add_streamer(self, login):
        user = self.twitch_client.user(login).data
        async with self.lock:
            await self.dbconn.execute("""
            INSERT INTO streamers
                (id, name, login, profile_image_url, offline_image_url, broadcaster_type, description, type)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
            """, int(user["id"]), user["display_name"], user["login"], user["profile_image_url"], user["offline_image_url"], user["broadcaster_type"], user["description"], user["type"])
        self.streamer_ids.add(int(user["id"]))
        self.streamer_logins.add(login)
    async def run(self, interval_seconds):
        last = datetime.datetime.now()
        interval = datetime.timedelta(seconds=interval_seconds)
        while True:
            await self._collect()
            now = datetime.datetime.now()
            if now <= last + interval:
                await asyncio.sleep((last + interval-now).total_seconds())
            last = last + interval
    async def _collect(self):
        streams = await self._streams()
        await self._update_games(streams)
        now = datetime.datetime.now()
        logined_streamers = [self.streamer_id_to_login[stream["user_id"]] for stream in streams]
        chatter_count, migrations = await self.chatter_manager.update(logined_streamers)
        chatter_count = {self.streamer_login_to_id[k]: v for k,v in chatter_count.items()}
        migrations = {(self.streamer_login_to_id[k[0]], self.streamer_login_to_id[k[1]]): v for k,v in migrations.items()}
        if migrations: 
            print(migrations)
        for (before, after), count in migrations.items():
            async with self.lock:
                await self.dbconn.execute(f"""
                    INSERT INTO chatter_migrations
                    (before, after, count, time) VALUES ($1, $2, $3, $4)"""
                    , before, after, count, now)
        for stream in streams:
            if stream:
                stream["chatter_count"] = chatter_count[stream["user_id"]]
                async with self.lock:
                    await self.dbconn.execute("""UPDATE streamers SET is_streaming = TRUE WHERE id = $1""", stream["id"])
                    cache = self.stream_change_cache.get(stream["user_id"])
                    if cache is None or cache["id"] != stream["id"]:
                        self.stream_change_cache[stream["id"]] = stream
                        cache = None
                        await self.dbconn.execute("""
                            INSERT INTO streams
                            (id, streamer_id, started_at)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (id) DO NOTHING; """,
                            stream["id"], stream["user_id"], stream["started_at"])
                    ALL_KEYS = ["game_id", "viewer_count", "language", "title", "chatter_count"]
                    update_keys = []
                    if cache is None:
                        update_keys = ALL_KEYS
                    else:
                        update_keys = [key for key in ALL_KEYS if cache[key] != stream[key]]
                    if update_keys:
                        #print(update_keys)
                        #print(f"update {stream}")
                        await self.dbconn.execute(f"""
                            INSERT INTO stream_changes
                            ({", ".join(update_keys)}, stream_id, time)
                            VALUES ( {", ".join( "$" + str(i+1) for i in range(len(update_keys)+2))} );
                            """, *[stream[i] for i in update_keys], stream["id"], now)
                        self.stream_change_cache[stream["user_id"]] = stream
            else:
                async with self.lock:
                    await self.dbconn.execute("""UPDATE streamers SET is_streaming = FALSE WHERE id = $1""", int(stream["id"]))
                    self.stream_change_cache[stream["user_id"]] = None
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
            stream["game_id"] = int(stream["game_id"])
            stream["viewer_count"] = int(stream["viewer_count"])
            stream["user_id"] = int(stream["user_id"])
            stream["started_at"] = datetime.datetime.fromisoformat(stream["started_at"][:-1])
        return streams
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
#asyncio.run(init(500, 60))
#asyncio.run(run(500, 60))
async def main(op):
    client_id = "6zqny3p0ft2js766jptev3mvp0ay51"
    db_args = dict(user='dbmasteruser',
            password='04zDuS5pq2uN6yH:&NvbU.{&5B4)xg7=',
            database='twitch_stats',
            host='ls-98524b0fb0c06c3883fc04035ec696d992806b8d.cheedxeegdzw.ap-northeast-2.rds.amazonaws.com',
            port=5432)
    async with Collector(client_id, db_args) as collector:
        print(op)
        if op == "init":
            await collector.init()
        elif op == "drop":
            await collector.drop()
        elif op == "run":
            await collector.run(60)
        elif op == "test":
            #await test_chatter_manager()
            #exit()
            try:
                '''
                await collector.add_streamer("saddummy")
                await collector.add_streamer("rhdgurwns")
                await collector.add_streamer("zilioner")
                await collector.add_streamer("trackingthepros")
                await collector.add_streamer("wltn4765")
                await collector.add_streamer("flurry1989")
                await collector.add_streamer("velvet_7")
                '''
            except Exception as e:
                pass
            await collector.run(60)

if __name__ == "__main__":
    import sys
    commands = ["init", "run", "drop", "test"]
    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print(f"""usage
    {sys.argv[0]} init
    {sys.argv[0]} run""")
        exit(1)
    asyncio.run(main(sys.argv[1]))
