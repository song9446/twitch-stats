from twitch import TwitchHelix
import asyncpg
import datetime
from itertools import islice


async def collects(num = 500, dbconn, client):
    now = datetime.datetime.now()
    reqs = [dbconn.execute("""
        INSERT INTO streams (streamer_id, time, game_id, viewer_count, language) VALUES ($1, $2, $3, $4, $5)
        """, stream["user_id"], now, stream["game_id"], stream["viewer_count"], stream["language"]) stream for stream in isslice(client.get_streams(page_size=100), 0, num)]
        #{'id': '36463417616', 'user_id': '157106641', 'user_name': 'PrimeEdd', 'game_id': '498566', 'type': 'live', 'title': 'Stake.com $5k Weekend Mega Race + Sports Betting', 'viewer_count': 300, 'started_at': datetime.datetime(2019, 12, 21, 12, 55, 1), 'language': 'en', 'thumbnail_url': 'https://static-cdn.jtvnw.net/previews-ttv/live_user_primeedd-{width}x{height}.jpg', 'tag_ids': ['6ea6bca4-4712-4ab9-a906-e3336a9d8039']}
        if rank > num:
            break

async def init(dbconn, client):
    await dbconn.execute("""
    CREATE DATABASE twitch_stats
    CREATE TABLE streamer (
        id INTEGER PRIMARY KEY,
        streamer_name TEXT NOT NULL,
        game_id INTEGER NOT NULL,
    );
    CREATE TABLE streams (
        id INTEGER PRIMARY KEY,
        streamer_id INTEGER REFERENCES streamer (id),
        game_id INTEGER NOT NULL,
        time TIMESTAMP NOT NULL DEFAULT NOW(),
        language TEXT NOT NULL
        viewer_count INTEGER NOT NULL DEFAULT 0
    );
    """)

async def update(stream, dbconn, client):



def run():
    client_id = "6zqny3p0ft2js766jptev3mvp0ay51"
    client = TwitchHelix(client_id=client_id)
    async with asyncpg.create_pool(user='postgres', 
            password='Thelifeisonlyonce', 
            database='twitch_stats', 
            host='127.0.0.1') as pool:
        async with pool.acquire() as conn:
            while True:
                update(stream, dbconn, client)
                await update_not_yet_born_characters(conn)
                await asyncio.sleep(0.1)
