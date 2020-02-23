import time
import asyncio
import aiohttp
import time

def split_into_even_size(lst, size):
    return [lst[i:i + size] for i in range(0, len(lst), size)]

class Helix:
    async def gen(client_id, client_secret=None, bearer_token=None):
        self = Helix()
        self.client_id = client_id
        self.client_secret = client_secret
        self.ratelimit_reset = None
        self.ratelimit_remaining = 800
        self.session = aiohttp.ClientSession(headers={
            'Client-ID': client_id,
            })
        self.headers = {}
        self.bearer_token_expired_at = time.time()-1
        await self.try_update_bearer_token()
        return self
    async def try_update_bearer_token(self):
        if time.time() < self.bearer_token_expired_at or not self.client_secret:
            return
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
        async with self.session.post("https://id.twitch.tv/oauth2/token", data=params) as res:
            now = time.time()
            oath = await res.json()
            self.bearer_token_expired_at = now + int(oath["expires_in"])
            self.headers['Authorization'] = "Bearer " + oath["access_token"]
    async def get(self, url):
        await self.try_update_bearer_token()
        if self.ratelimit_remaining == 0:
            await asyncio.sleep(self.ratelimit_reset - time.time())
        async with self.session.get(url, headers=self.headers) as res:
            if res.status == 429:
                self.ratelimit_remaining = 0
                await asyncio.sleep(self.ratelimit_reset - time.time())
                self.ratelimit_remaining = 800
                return await self.get(url)
            self.ratelimit_reset = float(res.headers["Ratelimit-Reset"])
            self.ratelimit_remaining = int(res.headers["Ratelimit-Remaining"])
            res = await res.json()
        return res
    async def close(self):
        await self.session.close()
    async def users(self, logins=[], ids=[]):
        ids = list(map(int, ids))
        params = logins + ids
        params_chunks = split_into_even_size(params, 100)
        res_chunks = await asyncio.gather(*(self.get(f"https://api.twitch.tv/helix/users?{'&'.join(('login=' if type(p) is str else 'id=') + str(p) for p in params)}") for params in params_chunks))
        return [d for res in res_chunks for d in res["data"]]
    async def games(self, ids):
        params_chunks = split_into_even_size(ids, 100)
        res_chunks = await asyncio.gather(*(self.get(f"https://api.twitch.tv/helix/games?{'&'.join('id='+str(param) for param in params)}") for params in params_chunks))
        return [d for res in res_chunks for d in res["data"]]
    async def _streams(self, url, min_viewer_count):
        data = []
        cursor = None
        while True:
            res = await self.get(url if not cursor else url + "&after=" + cursor)
            data.extend([d for d in res["data"] if int(d["viewer_count"]) >= min_viewer_count])
            if not res["data"] or int(res["data"][-1]["viewer_count"]) < min_viewer_count:
                break
            cursor = res["pagination"]["cursor"]
        return data
    async def streams(self, first=100, languages=[], user_ids=[], user_logins=[], min_viewer_count=25):
        if not user_ids and not user_logins:
            return (await self._streams(f"https://api.twitch.tv/helix/streams?first={first}" + "".join(f'&language={l}' for l in languages), min_viewer_count))
            #return (await self.get(f"https://api.twitch.tv/helix/streams?first={first}" + "".join(f'&language={l}' for l in languages)))["data"]
        user_ids = list(map(int, user_ids))
        params = user_logins + user_ids
        params_chunks = split_into_even_size(params, 100)
        res_chunks = await asyncio.gather(
                *(self.get(f"https://api.twitch.tv/helix/streams?first={first}" + ''.join(f'&language={l}' for l in languages) + ''.join((f'&user_login={p}' if type(p) is str else f'&user_id={p}') for p in params)) for params in params_chunks))
        return [d for res in res_chunks for d in res["data"]]
    async def following(self, from_id):
        return await self.get(f"https://api.twitch.tv/helix/users/follows?from_id={from_id}&first=100")
    async def followers(self, to_id, first=100):
        return await self.get(f"https://api.twitch.tv/helix/users/follows?to_id={to_id}&first={first}")
    async def chatters(self, channel):
        async with self.session.get(f"https://tmi.twitch.tv/group/user/{channel}/chatters") as resp:
            json = await resp.json()
            if len(json) == 0:
                return None
            viewers = list(json["chatters"]["viewers"])
            return viewers


async def test():
    client_args = {
        "client_id": "6v246n3kif7c42k8295imy2tvs6loq",
        "client_secret": "0gd9xfy66fa2zq6vps0a6fqqvcxp3z",
    }
    h = await Helix.gen(**client_args)
    #print(len(await h.streams(languages=["ko"], min_viewer_count = 25)))
    print(await h.followers(103825127, first=1))
    #print(await h.users(ids=[137734987]))
    #print(await h.streams(first=2, languages=["ko"], user_logins=["saddummy"]))
    #users = await h.games([0])
    #print(users)
    #print(await h.followers(30904062, 1))
if __name__ == "__main__":
    asyncio.run(test())

