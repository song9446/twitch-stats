from tenacity import retry, stop_after_attempt
import pickle
import api
from api import twitch_chat
from api import twitch_stream
import queue
import asyncio
import aiohttp
import threading
import time
import datetime
from shutil import copyfile
from util import split_into_even_size, ExpiredSet, MergedStream

class ChatManager:
    def __init__(self):
        #self.chat_counts = {}
        self.chats = {}
        self.queue = queue.Queue()
        self.ADD = 0
        self.REMOVE = 1
        self.EXTEND = 2
        self.SUBSTRACT = 3
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
                                #if login in self.chat_counts:
                                #    self.chat_counts.pop(login)
                                if login in self.chats:
                                    if login in self.chats:
                                        self.chats.pop(login)
                            elif op == self.SUBSTRACT:
                                logins = [l for l in login if l in twitch_chats]
                                for login in logins:
                                    if login in self.chats:
                                        self.chats.pop(login)
                                clients = [twitch_chats.pop(login) for login in logins]
                                res = await asyncio.gather(*[c.close() for c in clients])
                        except queue.Empty:
                            break
                try:
                    while True:
                        await dequeue()
                        async for msg in merged_stream:
                            await dequeue()
                            if msg.channel in twitch_chats:
                                #self.chat_counts.setdefault(msg.channel, 0)
                                #self.chat_counts[msg.channel] += 1
                                self.chats.setdefault(msg.channel, [])
                                self.chats[msg.channel].append(api.Chat(msg.user, msg.message))
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
    def substract(self, logins):
        self.queue.put_nowait((self.SUBSTRACT, logins))
    #def drain_chat_counts(self):
    #    chat_counts = self.chat_counts
    #    self.chat_counts = {}
    #    return chat_counts
    def drain_chats(self):
        chats = self.chats
        self.chats = {}
        return chats

class API(api.API):
    async def gen(client_args, language="ko", session_file_path="twitch_api_session.pkl"):
        self = API()
        self.session_file_path = session_file_path
        self.session = aiohttp.ClientSession()
        self.client_args = client_args
        self.twitch_client = await twitch_stream.Helix.gen(**self.client_args)
        self.chat_manager = ChatManager()
        self.language = language
        self.user_by_id = {}
        self.game_by_id = {}
        self.load_session()
        self.streaming_users = set()
        return self
    def load_session(self):
        try:
            with open(self.session_file_path, "rb") as f:
                session = pickle.load(f)
                self.user_by_id = session["user_by_id"]
                self.game_by_id = session["game_by_id"]
        except Exception as e:
            try:
                with open(self.session_file_path + ".back", "rb") as f:
                    session = pickle.load(f)
                    self.user_by_id = session["user_by_id"]
                    self.game_by_id = session["game_by_id"]
            except Exception as e:
                print(repr(e))
    def save_session(self):
        copyfile(self.session_file_path, self.session_file_path + ".back")
        with open(self.session_file_path, "wb") as f:
            session = {"user_by_id": self.user_by_id, "game_by_id": self.game_by_id}
            pickle.dump(session, f, protocol=-1)
    @retry(stop=stop_after_attempt(100))
    async def _chatters(self, channel):
        async with self.session.get(f"https://tmi.twitch.tv/group/user/{channel}/chatters") as resp:
            json = await resp.json()
            if len(json) == 0:
                return None
            viewers = list(json["chatters"]["viewers"])
            return viewers
    @retry(stop=stop_after_attempt(100))
    async def _users(self, user_ids):
        users = await self.twitch_client.users(ids=user_ids)
        return [api.User(
                id = int(user["id"]), 
                name = user["display_name"], 
                login = user["login"], 
                profile_image_url = user["profile_image_url"], 
                offline_image_url = user["offline_image_url"], 
                broadcaster_type = user["broadcaster_type"], 
                description = user["description"], 
                type = user["type"]) for user in users]
    @retry(stop=stop_after_attempt(100))
    async def _games(self, game_ids):
        games = await self.twitch_client.games(ids=game_ids)
        games = [api.Game(
            id = int(game["id"]),
            name = game["name"],
            box_art_url = game["box_art_url"],
            ) for game in games]
        return games
    @retry(stop=stop_after_attempt(100))
    async def _streams(self, user_ids):
        streams = await self.twitch_client.streams(user_ids=user_ids)
        unknown_game_ids = [s["game_id"] for s in streams if s["game_id"] and int(s["game_id"]) not in self.game_by_id]
        unknown_games = await self._games(unknown_game_ids)
        for game in unknown_games:
            self.game_by_id[game.id] = game
        streams = [api.Stream(
            id = int(s["id"]),
            game = (s["game_id"] and self.game_by_id[int(s["game_id"])]) or None,
            user = self.user_by_id[int(s["user_id"])] if int(s["user_id"]) in self.user_by_id else None,
            started_at = datetime.datetime.fromisoformat(s["started_at"][:-1] + "+00:00"),
            viewer_count = s["viewer_count"],
            title = s["title"],
            language = s["language"],
            chatters = [],
            chattings = [],
            ) for s in streams]
        streaming_users = set(s.user for s in streams)
        self.chat_manager.substract([u.login for u in (self.streaming_users - streaming_users)])
        self.chat_manager.extend([u.login for u in streaming_users])
        self.streaming_users = streaming_users
        chats = self.chat_manager.drain_chats()
        for s in streams:
            s.chatters = (await self._chatters(s.user.login)) or []
            s.chattings = chats.get(s.user.login, [])
        return streams
    @retry(stop=stop_after_attempt(100))
    async def _top100_streamers_update(self, min_viewers=100):
        streams = await self.twitch_client.streams(languages=[self.language], first=100)
        user_ids = [int(stream["user_id"]) for stream in streams if int(stream["viewer_count"]) >= min_viewers]
        users = await self._users(user_ids)
        for user in users:
            self.user_by_id[user.id] = user
    async def streams(self):
        print(1)
        if self.chat_manager.error:
            raise self.chat_manager.error
        print(2)
        await self._top100_streamers_update()
        print(3)
        streams = await self._streams([user.id for user in self.user_by_id.values()])
        self.save_session()
        return streams
