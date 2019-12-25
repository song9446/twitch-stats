import twitch
import datetime
import asyncio
import asyncpg
import contextlib

def split_into_even_size(lst, size):
    return [lst[i:i + size] for i in range(0, len(lst), size)]

class Collector:
    def __init__(self, client_id, db_args):
        self.client_id = client_id
        self.db_args = db_args
        self.stream_snapshot_cache = {}
    async def __aenter__(self):
        self.dbconn = await asyncpg.connect(**self.db_args)
        self.twitch_client = twitch.Helix(client_id = self.client_id, use_cache = False)
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
            CREATE TABLE stream_snapshots (
                stream_id BIGINT REFERENCES streams (id),
                viewer_count INTEGER NOT NULL DEFAULT 0,
                game_id BIGINT REFERENCES games (id),
                language CHAR(2), 
                time TIMESTAMP NOT NULL DEFAULT NOW(),
                PRIMARY KEY (stream_id, time)
            );
            """)
    async def drop(self):
        async with self.lock:
            await self.dbconn.execute("""
            DROP TABLE stream_snapshots;
            DROP TABLE streams; 
            DROP TABLE games; 
            DROP TABLE streamers; 
            """)
    async def streamer_ids(self):
        async with self.lock:
            return set(i["id"] for i in await self.dbconn.fetch("SELECT id FROM streamers"))
    async def add_streamer(self, login):
        user = self.twitch_client.user(login).data
        async with self.lock:
            await self.dbconn.execute("""
            INSERT INTO streamers 
                (id, name, login, profile_image_url, offline_image_url, broadcaster_type, description, type)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
            """, int(user["id"]), user["display_name"], user["login"], user["profile_image_url"], user["offline_image_url"], user["broadcaster_type"], user["description"], user["type"])
    async def run(self, interval):
        last = datetime.datetime.now()
        while True:
            await self._collect()
            now = datetime.datetime.now()
            await asyncio.sleep(interval - (now - last).total_seconds())
            last = now
    async def _collect(self):
        streams = await self._streams()
        await self._update_games(streams)
        now = datetime.datetime.now()
        print(f"collect {len(streams)}")
        for stream in streams:
            if stream:
                async with self.lock:
                    await self.dbconn.execute("""UPDATE streamers SET is_streaming = TRUE WHERE id = $1""", int(stream["id"]))
                    self.stream_snapshot_cache[int(stream["id"])] = stream
                    cache = self.stream_snapshot_cache.get(int(stream["user_id"]))
                    if cache is not None or cache["id"] != stream["id"]:
                        await self.dbconn.execute("""
                            INSERT INTO streams 
                            (id, streamer_id, started_at)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (id) DO NOTHING; """,
                            int(stream["id"]), int(stream["user_id"]), datetime.datetime.fromisoformat(stream["started_at"][:-1]))
                        self.stream_snapshot_cache.get(int(cache["id"]))
                    await self.dbconn.execute("""
                        INSERT INTO stream_snapshots 
                        (stream_id, time, game_id, viewer_count, language) 
                        VALUES ($1, $2, $3, $4, $5);
                        """, int(stream["id"]), now, int(stream["game_id"]) if stream["game_id"] else None, int(stream["viewer_count"]), stream["language"])
            else:
                async with self.lock:
                    await self.dbconn.execute("""UPDATE streamers SET is_streaming = FALSE WHERE id = $1""", int(stream["id"]))
    async def _streams(self):
        streamer_ids = await self.streamer_ids()
        streamer_id_chunks = split_into_even_size(list(streamer_ids), 100)
        streams = []
        for streamer_ids in streamer_id_chunks:
            try:
                streams.extend(self.twitch_client.streams(user_id=streamer_ids))
            except twitch.helix.StreamNotFound as e:
                pass
        return [i.data for i in streams]
    async def _update_games(self, streams):
        new_game_id_set = set()
        print(streams)
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
    db_args = dict(user='postgres', 
            password='Thelifeisonlyonce', 
            database='twitch_stats', 
            host='127.0.0.1')
    async with Collector(client_id, db_args) as collector:
        print(op)
        if op == "init":
            await collector.init()
        elif op == "drop":
            await collector.drop()
        elif op == "run":
            await collector.run(60)
        elif op == "test":
            #await collector.add_streamer("saddummy")
            #await collector.add_streamer("rhdgurwns")
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
