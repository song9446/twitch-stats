from api import afreeca
import asyncio

async def test_twitch_api():
    client_args = dict(client_id = "6zqny3p0ft2js766jptev3mvp0ay51",
                   bearer_token="pcjch55ezhaulaptylu85iq2ni4x6t")
    a = twitch.API(client_args)
    for i in range(10):
        streams = await a.streams()
        print(streams[0].user.name)
        print(streams[0].chattings)

async def test_afreeca_api():
    a = afreeca.API()
    for i in range(10000):
        streams = await a.streams()
        if streams: print(streams[0].user.name, streams[0].viewer_count , streams[0].chatter_count)
        if streams: print(len([u for u in list(streams[0].chatters) if u.is_female()]))
        print(i, "step")
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(test_afreeca_api())
