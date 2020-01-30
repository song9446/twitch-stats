from tenacity import retry, stop_after_attempt
import base64
import asyncio
import json
import time
import aiohttp
import random 
import collections

def split_into_even_size(lst, size):
    return [lst[i:i + size] for i in range(0, len(lst), size)]


PACKET_SPLIT = chr(12).encode()
PACKET_START = (chr(27) + chr(9)).encode()
LOG_SPLIT = chr(6)

SVC_CHAT = 5
SVC_MOVE = 4
SVC_FLAGCHANGE = 12

USERTYPE_USER = 0
USERTYPE_STAFF = 1
USERTYPE_POLICE = 2





class User:
    def __init__(self, id, name, flags=(), type=USERTYPE_USER):
        self.id = id
        self.name = name
        self.flags = (flags[0] if len(flags) > 0 else 0, flags[1] if len(flags) > 1 else 0)
        self.type = type
    def __hash__(self):
        return hash(self.id)
    def __eq__(self, other):
        return self.id == other.id
    def is_admin(self):
        return self.flags[0] & 1
    def is_hidden(self):
        return self.flags[0] & 2
    def is_bj(self):
        return self.flags[0] & 4
    def is_dumb(self):
        return self.flags[0] & 8
    def is_guest(self):
        return self.flags[0] & 16
    def is_fanclub(self):
        return self.flags[0] & 32
    def is_automanager(self):
        return self.flags[0] & 64
    def is_manager_list(self):
        return self.flags[0] & 128
    def is_manager(self):
        return self.flags[0] & 256
    def is_female(self):
        return self.flags[0] & 512
    def is_exituser(self):
        return self.flags[0] & 8192
    def is_mobile(self):
        return self.flags[0] & 16384
    def is_topfan(self):
        return self.flags[0] & 32768
    def is_quickview(self):
        return self.flags[0] & (1 << 19)
    def is_follower(self):
        return self.flags[0] & (1 << 28)
    def is_follower6(self):
        return self.flags[1] & (1 << 18)
    def is_follower12(self):
        return self.flags[1] & (1 << 19)
"""[
        ADMIN: {
            where: 1,
            value: 1
            },
        HIDDEN: {
            where: 1,
            value: 2
            },
        BJ: {
            where: 1,
            value: 4
            },
        DUMB: {
            where: 1,
            value: 8
            },
        GUEST: {
            where: 1,
            value: 16
            },
        FANCLUB: {
            where: 1,
            value: 32
            },
        AUTOMANAGER: {
            where: 1,
            value: 64
            },
        MANAGERLIST: {
            where: 1,
            value: 128
            },
        MANAGER: {
            where: 1,
            value: 256
            },
        FEMALE: {
            where: 1,
            value: 512
            },
        AUTODUMB: {
            where: 1,
            value: 1024
            },
        DUMB_BLIND: {
            where: 1,
            value: 2048
            },
        DOBAE_BLIND: {
            where: 1,
            value: 4096
            },
        DOBAE_BLIND2: {
                where: 1,
                value: 1 << 24
                },
        EXITUSER: {
                where: 1,
                value: 8192
                },
        MOBILE: {
                where: 1,
                value: 16384
                },
        TOPFAN: {
                where: 1,
                value: 32768
                },
        REALNAME: {
                where: 1,
                value: 65536
                },
        NODIRECT: {
                where: 1,
                value: 1 << 17
                },
        GLOBAL_APP: {
                where: 1,
                value: 1 << 18
                },
        QUICKVIEW: {
                where: 1,
                value: 1 << 19
                },
        SPTR_STICKER: {
                where: 1,
                value: 1 << 20
                },
        CHROMECAST: {
                where: 1,
                value: 1 << 21
                },
        FOLLOWER: {
                where: 1,
                value: 1 << 28
                },
        NOTIVODBALLOON: {
                where: 1,
                value: 1 << 30
                },
        NOTITOPFAN: {
                where: 1,
                value: 1 << 31
                },
        GLOBAL_PC: {
                where: 2,
                value: 1
                },
        CLAN: {
                where: 2,
                value: 2
                },
        TOPCLAN: {
                where: 2,
                value: 4
                },
        TOP20: {
                where: 2,
                value: 8
                },
        GAMEGOD: {
                where: 2,
                value: 16
                },
        GAMEIMO: {
                where: 2,
                value: 32
                },
        NOSUPERCHAT: {
                where: 2,
                value: 64
                },
        NORECVCHAT: {
                where: 2,
                value: 128
                },
        FLASH: {
                where: 2,
                value: 256
                },
        LGGAME: {
                where: 2,
                value: 512
                },
        EMPLOYEE: {
                where: 2,
                value: 1024
                },
        CLEANATI: {
                where: 2,
                value: 2048
                },
        POLICE: {
                where: 2,
                value: 4096
                },
        ADMINCHAT: {
                where: 2,
                value: 8192
                },
        PC: {
                where: 2,
                value: 16384
                },
        SPECIFY: {
                where: 2,
                value: 32768
                },
        FOLLOWER_6: {
                where: 2,
                value: 1 << 18
                },
        FOLLOWER_12: {
                where: 2,
                value: 1 << 19
                }
        },
            ]"""

class Message:
    def __init__(self, raw, bjid):
        self.bjid = bjid
        svc, ret, packet = parse(raw)
        self.svc = svc
        if svc == SVC_CHAT:
            self.user = User(packet[1] , packet[5], tuple(map(int, packet[6].split('|'))), packet[3])
            self.message = packet[0]
        elif svc == SVC_MOVE:
            self.users = [User(p[0], p[1], tuple(map(int, filter(lambda x:x, p[2].split('|'))))) for p in split_into_even_size(packet[1:], 3) if len(p) == 3]
            self.is_join = int(packet[0]) > 0
        elif svc == SVC_FLAGCHANGE:
            self.oldflag = packet[5]
            self.flag = packet[0]
            self.user = User(packet[1] , packet[2], tuple(map(int, packet[0].split('|'))))
        else:
            self.raw = (svc, ret, packet)
    def __str__(self):
        if self.svc == SVC_CHAT:
            return f'{self.user.name}({self.user.type}):{self.message} | {self.user.is_guest()}'
        elif self.svc == SVC_MOVE:
            for user in self.users:
                return f'{user.name} {"in" if self.is_join else "out"} | {user.is_guest()}' 
        elif self.svc == SVC_FLAGCHANGE:
            return f'{self.user.name} change | {self.user.flags}' 
        else:
            return str(self.raw)


def makePacket(svc, ret, body):
    packet = b''.join([PACKET_SPLIT, PACKET_SPLIT.join(map(str.encode, map(str, body)))])
    return b''.join([PACKET_START, str(svc).zfill(4).encode(), str(len(packet)).zfill(6).encode(), str(ret).zfill(2).encode(), packet])
def readBody(e):
    return [s.decode() for s in e[1:].split(PACKET_SPLIT)]
def parse(e):
    if not e:
        raise Exception(e)
    svc = int(e[2:6])
    packet_size = int(e[6:12])
    ret = int(e[12:14])
    packet = readBody(e[14:])
    if ret>0:
        raise Exception(svc, ret, packet)
    return (svc, ret, packet)
def greetingPacket():
    return makePacket(1, 0, ['', '', '16', ''])
def loginPacket(metadata):
    return makePacket(2, 0, [metadata["CHATNO"], metadata["FTK"],  '0', '', '', ''])
def pingPacket():
    return makePacket(0, 0, [])

class Client:
    def __init__(self, retry=10):
        self.bjid = None
        self.retry = retry
        self.last_ping_time = time.time()
        self.PING_INTERVAL = 60
        self.is_streaming = False
        #self.chatters = set()
        self.session = None
        self.ws = None
    @retry(stop=stop_after_attempt(10))
    async def connect(bjid, bno):
        c = Client()
        await c.init(bjid, bno)
        return c
    async def init(self, bjid, bno):
        self.bjid = bjid
        self.bno = bno
        self.session = aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"}, timeout=aiohttp.ClientTimeout(total=5))
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Host": "live.afreecatv.com",
            "Origin": "http://play.afreecatv.com",
            "Referer": f"http://play.afreecatv.com/{bjid}/{bno}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"
        }
        form = {
            "bid": bjid,
            "bno": bno,
            "type": "live",
            "pwd": '',
            "player_type": "html5",
            "stream_type": "common",
            "quality": "HD",
            "mode": "landing",
        }
        async with self.session.post("http://live.afreecatv.com/afreeca/player_live_api.php?bjid=" + bjid, data=form, headers=headers) as res:
            metadata = json.loads(await res.text())["CHANNEL"]
        if metadata["RESULT"] == 0:
            self.is_streaming = False
            return None
        else:
            self.is_streaming = True
        self.ws = await self.session.ws_connect(f"ws://{metadata['CHIP']}:{metadata['CHPT']}/Websocket", protocols=("chat",))
        await self.ws.send_bytes(greetingPacket())
        await self.ws.receive()
        await self.ws.send_bytes(loginPacket(metadata))
        await self.ws.receive()
    async def close(self):
        if self.ws:
            await self.ws.close()
        if self.session:
            await self.session.close()
    def __aiter__(self):
        return self
    async def __anext__(self):
        if not self.is_streaming:
            raise StopAsyncIteration
        now = time.time()
        if self.last_ping_time + self.PING_INTERVAL <= now:
            await self.ws.send_bytes(pingPacket())
            self.last_ping_time = now
        msg = await self.ws.receive()
        error = False
        if msg.type == aiohttp.WSMsgType.BINARY:
            if not msg.data:
                error = True
            else:
                msg = Message(raw=msg.data, bjid=self.bjid)
                if msg.svc == SVC_CHAT or msg.svc == SVC_MOVE or msg.svc == SVC_FLAGCHANGE:
                    return msg
                else:
                    return await self.__anext__()
        elif msg.type == aiohttp.WSMsgType.ERROR:
            error = True
        if error:
            if self.retry > 0:
                self.retry -= 1
                await self.close()
                await self.__init(self.bj_id, self.broad_no)
                return await self.__anext__()
            else:
                raise StopAsyncIteration

async def test():
    c = await Client.connect("phonics1", 220522681)
    async for msg in c:
        print(msg)

if __name__ == "__main__":
    asyncio.run(test())
