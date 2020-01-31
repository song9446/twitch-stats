from tenacity import retry, stop_after_attempt, retry_if_exception_type
import json
import pickle
import api
from api import afreeca_chat, afreeca_stream
import queue
import asyncio
import aiohttp
import threading
import time
import datetime
from util import split_into_even_size, ExpiredSet, MergedStream, limited_api

class ChatManager:
    def __init__(self):
        #self.chat_counts = {}
        self.chats = {}
        self.chatters = {}
        self.queue = queue.Queue()
        self.ADD = 0
        self.REMOVE = 1
        self.EXTEND = 2
        self.SUBSTRACT = 3
        self.error = None
        self._wakeup = asyncio.Event()
        async def connect(bjid, bno, afreeca_chats, merged_stream):
            c = await afreeca_chat.Client.connect(bjid, bno)
            afreeca_chats[bjid] = c
            merged_stream.append(c)
        async def connect_all(args, afreeca_chats, merged_stream):
            await limited_api(10, 0.1, [connect(bjid, bno, afreeca_chats, merged_stream) for bjid, bno in args])
            return None
        def run(self):
            async def _run():
                merged_stream = MergedStream()
                afreeca_chats = {}
                async def dequeue():
                    while True:
                        try:
                            op, login = self.queue.get_nowait()
                            if op == self.EXTEND:
                                args = [(bjid, bno) for bjid, bno in login if bjid not in afreeca_chats]
                                for bjid, bno in args:
                                    afreeca_chats[bjid] = None
                                merged_stream.append_future(connect_all(args, afreeca_chats, merged_stream))
                                #merged_stream.append_future(limited_api(10, 0.1, [connect(bjid, bno, afreeca_chats, merged_stream) for bjid, bno in login]))
                                #res = await asyncio.gather(*[afreeca_chat.Client.connect(bjid, bno) for bjid, bno in logins])
                                #res = await limited_api(10, 0.1, [afreeca_chat.Client.connect(bjid, bno) for bjid, bno in logins])
                                #for i, (bjid, bno) in enumerate(logins):
                                #    afreeca_chats[bjid] = res[i]
                                #    merged_stream.append(res[i])
                            elif op == self.REMOVE:
                                if bjid in afreeca_chats:
                                    await afreeca_chats.pop(bjid).close()
                                #if login in self.chat_counts:
                                #    self.chat_counts.pop(login)
                                if bjid in self.chats:
                                    if bjid in self.chats:
                                        self.chats.pop(bjid)
                            elif op == self.SUBSTRACT:
                                logins = [(bjid, bno) for bjid, bno in login if bjid in afreeca_chats]
                                for bjid, bno in logins:
                                    if bjid in self.chats:
                                        self.chats.pop(bjid)
                                clients = [afreeca_chats.pop(bjid) for bjid, bno in logins]
                                res = await asyncio.gather(*[c.close() for c in clients])
                        except queue.Empty:
                            break
                try:
                    while True:
                        self._wakeup.clear()
                        await merged_stream.available()
                        self._wakeup.set()
                        await dequeue()
                        async for msg in merged_stream:
                            self._wakeup.clear()
                            await merged_stream.available()
                            self._wakeup.set()
                            await dequeue()
                            if msg.bjid in afreeca_chats:
                                self.chatters.setdefault(msg.bjid, set())
                                if msg.svc == afreeca_chat.SVC_CHAT:
                                    self.chatters[msg.bjid].add(msg.user)
                                    self.chats.setdefault(msg.bjid, [])
                                    self.chats[msg.bjid].append(api.Chat(
                                        user_id=msg.user.id, 
                                        chat=msg.message, 
                                        is_follower=msg.user.is_follower(),
                                        is_fanclub = msg.user.is_fanclub(),
                                        is_topfan = msg.user.is_topfan(),
                                        is_female = msg.user.is_female(),))
                                elif msg.svc == afreeca_chat.SVC_MOVE:
                                    if msg.is_join:
                                        self.chatters[msg.bjid].update(msg.users)
                                    else:
                                        self.chatters[msg.bjid].difference_update(msg.users)
                                elif msg.svc == afreeca_chat.SVC_FLAGCHANGE:
                                    if msg.bjid in self.chatters:
                                        self.chatters[msg.bjid].remove(msg.user)
                                    self.chatters[msg.bjid].add(msg.user)
                                #self.chat_counts.setdefault(msg.channel, 0)
                                #self.chat_counts[msg.channel] += 1
                        await asyncio.sleep(1)
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
    def get_chatters(self, bjid):
        return list(self.chatters.get(bjid) or [])
    async def wait_available(self):
        await self._wakeup.wait()

class StreamManager:
    def __init__(self):
        self.streams = {}
        self.queue = queue.Queue()
        self.ADD = 0
        self.REMOVE = 1
        self.EXTEND = 2
        self.SUBSTRACT = 3
        self.error = None
        self.closed_clients = []
        self._wakeup = asyncio.Event()
        async def connect(bjid, streams, merged_stream):
            try:
                c = await afreeca_stream.Client.connect(bjid)
                streams[bjid] = c
                merged_stream.append(c)
            except afreeca_stream.ForignerDenied as e:
                #print(repr(e))
                streams.pop(bjid)
                return None
            except afreeca_stream.DoNotStreaming as e:
                #print(repr(e))
                streams.pop(bjid)
                return None
            except afreeca_stream.AdultDenied as e:
                #print(repr(e))
                streams.pop(bjid)
                return None
            except Exception as e:
                raise e
        async def connect_all(args, streams, merged_stream):
            await limited_api(10, 0.1, [connect(bjid, streams, merged_stream) for bjid in args])
            return None
        def run(self):
            async def _run():
                merged_stream = MergedStream()
                self.streams = {}
                async def dequeue():
                    while True:
                        try:
                            op, login = self.queue.get_nowait()
                            if op == self.EXTEND:
                                args = [bjid for bjid in login if bjid not in self.streams]
                                for bjid in args:
                                    self.streams[bjid] = None
                                merged_stream.append_future(connect_all(args, self.streams, merged_stream))
                                #logins = [bjid for bjid in set(login) if bjid not in self.streams]
                                #res = await asyncio.gather(*[afreeca_stream.Client.connect(bjid) for bjid in logins])
                                #res = await limited_api(10, 0.1, [connect(bjid) for bjid in logins])
                                #for i, bjid in enumerate(logins):
                                #    if res[i]:
                                #        self.streams[bjid] = res[i]
                                #        merged_stream.append(res[i])
                            elif op == self.SUBSTRACT:
                                logins = [bjid for bjid in login if bjid in self.streams]
                                for bjid in logins:
                                    if bjid in self.chats:
                                        self.chats.pop(bjid)
                                clients = [self.streams.pop(bjid) for bjid in logins]
                                res = await asyncio.gather(*[c.close() for c in clients])
                        except queue.Empty:
                            break
                try:
                    while True:
                        self._wakeup.clear()
                        await merged_stream.available()
                        self._wakeup.set()
                        await dequeue()
                        async for msg in merged_stream:
                            self._wakeup.clear()
                            await merged_stream.available()
                            self._wakeup.set()
                            await dequeue()
                            if msg.bjid in self.streams:
                                if msg.svc == "CLOSECH":
                                    self.closed_clients.append(self.streams.pop(msg.bjid))
                                else:
                                    pass
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
    def drain_closed_clients(self):
        closed = self.closed_clients
        self.closed_clients = []
        return closed
    def stream(self, bjid):
        return self.streams.get(bjid)
    async def wait_available(self):
        await self._wakeup.wait()

class API(api.API):
    def __init__(self, session_file_path="afreeca_api_session.pkl"):
        self.session_file_path = session_file_path
        self.session = aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"}, timeout=aiohttp.ClientTimeout(total=5))
        self.chat_manager = ChatManager()
        self.stream_manager = StreamManager()
        self.user_by_id = {}
        self.load_session()
        self.streaming_users = set()
    def load_session(self):
        try:
            with open(self.session_file_path, "rb") as f:
                session = pickle.load(f)
                self.user_by_id = session["user_by_id"]
        except Exception as e:
            print(repr(e))
    def save_session(self):
        with open(self.session_file_path, "wb") as f:
            session = {"user_by_id": self.user_by_id}
            pickle.dump(session, f, protocol=-1)
    async def _chatters(self, bjid):
        return self.chat_manager.get_chatters(bjid)
    async def _get(self, url):
        async with self.session.get(url, timeout=timeout) as res:
            return await res.text()
    @retry(stop=stop_after_attempt(10), retry=retry_if_exception_type(asyncio.TimeoutError))
    async def _user(self, user_id):
        async with self.session.get(f"http://bjapi.afreecatv.com/api/{user_id}/station") as res:
            return json.loads(await res.text())
    async def _users(self, user_ids):
        user_id_chunks = split_into_even_size(user_ids, 10)
        #user_chunks = [(await asyncio.gather(*[self._user(user_id) for user_id in user_ids])) for user_ids in user_id_chunks]
        user_chunks = [(await limited_api(10, 0.1, [self._user(user_id) for user_id in user_ids])) for user_ids in user_id_chunks]
        users = [user for users in user_chunks for user in users]
        return [api.User(
                id = user["station"]["user_id"],
                login = user["station"]["user_id"], 
                name = user["station"]["user_nick"],
                profile_image_url = user["profile_image"].replace("\/", "/")[2:], 
                type = ",".join(filter(lambda x: x, (("best" if user["is_best_bj"] else None), ("partner" if user["is_partner_bj"] else None), ("sports" if user["is_sports_bj"] else None)))),
                description = user["station"]["display"]["profile_text"]) for user in users]
    async def _streams(self, user_ids):
        await self.stream_manager.wait_available()
        self.stream_manager.extend(user_ids)
        streams = [self.stream_manager.stream(id) for id in user_ids]
        streams = [api.Stream(
            id = int(s.bno),
            game = s.category,
            user = self.user_by_id[s.bjid] if s.bjid in self.user_by_id else None,
            started_at = s.started_at,
            main_viewer_count = s.main_pc_users + s.main_mobile_users,
            sub_viewer_count = s.sub_pc_users + s.sub_mobile_users,
            join_viewer_count = s.join_pc_users + s.join_mobile_users,
            viewer_count = s.main_pc_users + s.main_mobile_users + s.sub_pc_users + s.sub_mobile_users,
            title = s.title,
            language = "ko",
            chatters = [],
            chattings = [],
            ) for s in streams if s and s.main_pc_users >= 0]
        await self.chat_manager.wait_available()
        self.chat_manager.extend([(s.user.login, s.id) for s in streams])
        closed_clients = self.stream_manager.drain_closed_clients()
        await self.chat_manager.wait_available()
        self.chat_manager.substract([(s.bjid, s.bno) for s in closed_clients])
        chats = self.chat_manager.drain_chats()
        for s in streams:
            s.chatters = (await self._chatters(s.user.login)) or []
            s.chattings = chats.get(s.user.login) or []
            in_main_ch_room = (s.join_viewer_count == s.main_viewer_count)
            if in_main_ch_room:
                s.chatter_count = round(s.viewer_count * ((len(s.chatters) / s.join_viewer_count) if s.join_viewer_count else 0))
            else:
                s.chatter_count = s.main_viewer_count + round((s.viewer_count - s.main_viewer_count) * ((len(s.chatters) / s.join_viewer_count) if s.join_viewer_count else 0))
        return streams
    async def _top_streamers_update(self, streamers = 120, min_viewers=100):
        milli_timestamp = int(time.time() * 1000)
        user_ids = []
        for page in range(1, int((streamers-1)/60)+2):
            url = f"http://live.afreecatv.com/api/main_broad_list_api.php?selectType=action&orderType=view_cnt&pageNo={page}&lang=ko_KR&_={milli_timestamp}"
            async with self.session.get(url) as res:
                res = json.loads((await res.text())[1:-2])
            for b in res["broad"]:
                if int(b["total_view_cnt"]) > min_viewers:
                    user_ids.append(b["user_id"])
                    if len(user_ids) > streamers:
                        break
            if len(user_ids) > streamers:
                break
        users = await self._users(user_ids)
        for user in users:
            self.user_by_id[user.login] = user
    async def streams(self):
        if self.chat_manager.error:
            raise self.chat_manager.error
        if self.stream_manager.error:
            raise self.stream_manager.error
        await self._top_streamers_update(100)
        streams = await self._streams([user.login for user in self.user_by_id.values()])
        self.save_session()
        return streams

async def test():
    api = API()
    while True:
        print(api.streams())
        print(datetime.datetime.now())
