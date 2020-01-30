import asyncio
import json
import time
import aiohttp
import random 
import collections

class Message:
    def __init__(self, raw, bj_id):
        msg = raw.split(b'|')
        self.op = msg[0].decode()
        self.bj_id=bj_id
        self.user_id = msg[2].decode()
        self.chat=msg[1].decode()
class Client:
    def __init__(self, retry=10):
        self.session = None
        self.ws = None
        self.message_queue = collections.deque()
        self.room_state = {}
        self.channel = None
        self.retry = retry
        self.last_ping_time = time.time()
        self.PING_INTERVAL = 60*5
        #self.chatters = set()
    async def connect(bj_id, broad_no):
        client = Client()
        await client.__init(bj_id, broad_no)
        return client
    async def close(self):
        await self.ws.close()
        await self.session.close()
    async def __fetch_meta(self, bj_id, broad_no):
        form = {
                "bj_id": bj_id, 
                "broad_no": broad_no,
                "language": "ko",
                "agent": "web",
                }
        async with self.session.post("http://api.m.afreecatv.com/broad/a/watch", data=form) as res:
            return json.loads(await res.text())["data"]
    async def __init(self, bj_id, broad_no):
        self.bj_id = bj_id
        self.broad_no = broad_no
        self.session = aiohttp.ClientSession()
        self.metadata = await self.__fetch_meta(bj_id, broad_no)
        print(self.metadata)
        self.ws = await self.session.ws_connect("ws://218.38.31.147:3579/Websocket", protocols=('chat',))
        await self.ws.send_str(f'LOGIN{self.metadata["channel_info"]}::0::mweb_aos')
        await self.ws.receive()
        await self.ws.send_str(f'JOIN{self.metadata["chat_no"]}:{self.metadata["fan_ticket"]}')
        await self.ws.receive()
        await self.ws.send_str(f'CONNECTCENTER{self.metadata["relay_ip"]}:{self.metadata["relay_port"]}:{self.metadata["broad_no"]}')
        await self.ws.receive()
    def __aiter__(self):
        return self
    async def __anext__(self):
        #now = time.time()
        #if self.last_ping_time + self.PING_INTERVAL <= now:
        #    print("send ping")
        #    await self.ws.send_str("PING")
        #    self.last_ping_time = now
        msg = await self.ws.receive()
        if msg.type == aiohttp.WSMsgType.BINARY:
            print(msg.data)
            msg = Message(raw=msg.data, bj_id=self.bj_id)
            if msg.op == 'CHATMESG':
                return msg
            else:
                return await self.__anext__()
        elif msg.type == aiohttp.WSMsgType.ERROR:
            if self.retry > 0:
                self.retry -= 1
                await self.close()
                await self.__init(self.bj_id, self.broad_no)
                return await self.__anext__()
            else:
                raise StopAsyncIteration

async def test():
    cs = await Client.connect("phonicsl", 220522681)
    async for msg in cs:
        pass
        #if msg: print(msg.bj_id, msg.user_id, msg.chat)

    #msg = await asyncio.wait_for(c.__anext__(), timeout=0.0001)
    #print(msg.type, msg.channel, msg.user, msg.message)
    #async for msg in cs:
    #await asyncio.wait_for([async for msg in c:

if __name__ == "__main__":
    asyncio.run(test())
