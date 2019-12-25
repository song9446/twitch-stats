#from twitch import TwitchHelix
from twitch import TwitchHelix
import asyncpg
import asyncio
import datetime
from itertools import islice

class Collector:
    def __init__(self, client_id, db_args):
        self.client_id = client_id
        self.db_args = db_args
    async def __aenter__(self):
        self.dbconn = await asyncpg.connect(**self.db_args)
        '''
        self.twitch_client = TwitchHelix(client_id=self.client_id)
        try:
            self.game_id_set = set(i["id"] for i in await self.dbconn.fetch("SELECT id FROM games"))
            self.streamer_id_set = set(i["id"] for i in await self.dbconn.fetch("SELECT id FROM streamers"))
        except Exception as e:
            print(e)
            self.game_id_set = set()
            self.streamer_id_set = set()
        self.lock = asyncio.Lock()
        return self
        '''
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
                name TEXT, 
                login TEXT,
            );
            CREATE TABLE games (
                id BIGINT PRIMARY KEY,
                name TEXT,
                box_art_url TEXT
            );
            CREATE TABLE streams (
                id BIGINT PRIMARY KEY,
                streamer_id BIGINT REFERENCES streamers (id),
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            );
            CREATE TABLE stream_states (
                stream_id BIGINT REFERENCE streams (id),
                game_id BIGINT REFERENCES games (id),
                language TEXT, 
                time TIMESTAMP NOT NULL DEFAULT NOW(),
            );
            CREATE TABLE stream_viewer_counts (
                stream_id BIGINT REFERENCE streams (id),
                time TIMESTAMP NOT NULL DEFAULT NOW(),
                streamer_id BIGINT REFERENCES streamers (id),
                game_id BIGINT REFERENCES games (id),
                language TEXT NOT NULL,
                viewer_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (stream_id, time)
            );
            """)

    async def drop(self):
        async with self.lock:
            await self.dbconn.execute("""
            DROP TABLE stream_snapshots;
            DROP TABLE streamers; 
            DROP TABLE games; 
            """)

    async def run(self, num, interval):
        last = datetime.datetime.now()
        async with self.lock:
            while True:
                await self._collect(num)
                now = datetime.datetime.now()
                await asyncio.sleep(interval - (now - last).total_seconds())
                last = now
    async def _collect(self, num):
        now = datetime.datetime.now()
        new_game_id_set = set()
        streams = [stream for stream in islice(self.twitch_client.get_streams(page_size=100), 0, num)]
        for stream in streams:
            if stream["game_id"] and (int(stream["game_id"]) not in self.game_id_set):
                new_game_id_set.add(int(stream["game_id"]))
        new_game_ids = list(new_game_id_set)
        chunks = [new_game_ids[i:i + 100] for i in range(0, len(new_game_ids), 100)]
        for chunk in chunks:
            games = self.twitch_client.get_games(game_ids=chunk)
            for game in games:
                await self.dbconn.execute("""
                    INSERT INTO games (id, name, box_art_url) VALUES ($1, $2, $3)
                    """, int(game["id"]), game["name"], game["box_art_url"])
        self.game_id_set = self.game_id_set.union(new_game_id_set)
        for stream in streams:
            if int(stream["user_id"]) not in self.streamer_id_set:
                await self.dbconn.execute("""
                    INSERT INTO streamers (id, name) VALUES ($1, $2)
                    """, int(stream["user_id"]), stream["user_name"])
                self.streamer_id_set.add(int(stream["user_id"]))
            await self.dbconn.execute("""
                INSERT INTO stream_snapshots (stream_id, streamer_id, time, game_id, viewer_count, language) VALUES ($1, $2, $3, $4, $5, $6);
                """, int(stream["id"]), int(stream["user_id"]), now, int(stream["game_id"]) if stream["game_id"] else None, int(stream["viewer_count"]), stream["language"])
            #{'id': '36463417616', 'user_id': '157106641', 'user_name': 'PrimeEdd', 'game_id': '498566', 'type': 'live', 'title': 'Stake.com $5k Weekend Mega Race + Sports Betting', 'viewer_count': 300, 'started_at': datetime.datetime(2019, 12, 21, 12, 55, 1), 'language': 'en', 'thumbnail_url': 'https://static-cdn.jtvnw.net/previews-ttv/live_user_primeedd-{width}x{height}.jpg', 'tag_ids': ['6ea6bca4-4712-4ab9-a906-e3336a9d8039']}
        print("collected..")

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
            await collector.run(500, 60)

if __name__ == "__main__":
    import sys
    commands = ["init", "run", "drop"]
    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print(f"""usage
    {sys.argv[0]} init
    {sys.argv[0]} run""")
    asyncio.run(main(sys.argv[1]))
