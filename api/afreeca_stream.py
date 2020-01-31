from tenacity import retry, stop_after_attempt, retry_if_exception_type
import api
import datetime
import base64
import asyncio
import json
import time
import aiohttp
import random 
import collections

def split_into_even_size(lst, size):
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def naive_parse(text, start_token, end_token, start_index=0):
    i = text.find(start_token, start_index)
    if i<0: return -1, start_token[0:0]
    else: i += len(start_token)
    j = text.find(end_token, i)
    if j<0: return -1, start_token[0:0]
    return i, text[i:j]



def gen_uid():
    n = "0123456789ABCDEF"
    i = [''] * 36
    r = 0
    for e in range(8):
        i[r] = n[random.randint(0, 15)]
        r += 1
    for e in range(3):
        for t in range(4):
            i[r] = n[random.randint(0, 15)]
            r += 1
    a = ("0000000" + hex(int(time.time()*1000))[2:].upper())[-8:]
    for e in range(8):
        i[r] = a[e]
        r += 1
    for e in range(4):
        i[r] = n[random.randint(0, 15)]
        r += 1
    return "".join(i)


class Message:
    def __init__(self, raw, bjid):
        self.bjid = bjid
        msg = json.loads(raw)
        if msg["RESULT"] != 0:
            self.success = False
        else:
            self.success = True
        self.svc = msg["SVC"]
        if msg["SVC"] == "GETUSERCNTEX":
            self.main_pc_users = msg["DATA"]["uiMainChPCUser"]
            self.main_mobile_users = msg["DATA"]["uiMainChMBUser"]
            self.sub_pc_users = msg["DATA"]["uiSubChPCUser"]
            self.sub_mobile_users = msg["DATA"]["uiSubChMBUser"]
            self.join_pc_users = msg["DATA"]["uiJoinChPCUser"]
            self.join_mobile_users = msg["DATA"]["uiJoinChMBUser"]
        elif msg["SVC"] == "GETUSERCNT":
            self.main_pc_users = msg["DATA"]["uiJoinChUser"]
            self.main_mobile_users = msg["DATA"]["uiMbUser"]
            self.sub_pc_users = 0
            self.sub_mobile_users = 0
            self.join_pc_users = msg["DATA"]["uiJoinChUser"]
            self.join_mobile_users = msg["DATA"]["uiMbUser"]
        elif msg["SVC"] == "CLOSECH":
            self.ending_type = msg["DATA"]["iEndingType"]
        else:
            pass
    def __str__(self):
        if self.svc == "GETUSERCNTEX" or self.svc == "GETUSERCNT":
            return f"{self.main_pc_users}, {self.main_mobile_users}, {self.sub_pc_users}, {self.sub_mobile_users}"




class ForignerDenied(Exception):
    pass
class DoNotStreaming(Exception):
    pass
class AdultDenied(Exception):
    pass


class Client:
    def __init__(self, retry=10):
        self.bjid = None
        self.retry = retry
        self.last_ping_time = time.time()
        self.is_streaming = False
        self.PING_INTERVAL = 30
        self.main_pc_users = -1
        self.main_mobile_users = -1
        self.sub_pc_users = -1
        self.sub_mobile_users = -1
        self.join_pc_users = -1
        self.join_mobile_users = -1
        self.ws = None
        self.session = None
        #self.chatters = set()
    #@retry(stop=stop_after_attempt(10), )
    async def connect(bjid):
        c = Client()
        await c.init(bjid)
        return c
    @retry(stop=stop_after_attempt(10), retry=(retry_if_exception_type(asyncio.TimeoutError) | retry_if_exception_type(aiohttp.ClientResponseError)))
    async def init(self, bjid):
        self.bjid = bjid
        self.session = aiohttp.ClientSession(
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"}, 
                cookies={"UserClipToolTip":"off", "AbroadChk": "FAIL", "AbroadVod": "FAIL"},
                timeout=aiohttp.ClientTimeout(total=5))
        async with self.session.get(f"http://play.afreecatv.com/{bjid}/null") as res:
            html = await res.text()
        if len(html) < 1000:
            i, a = naive_parse(html, '</iframe><script>document.getElementById("f").src="', '"</script>')
            a = a.replace('"+Date.now()+"', str(int(time.time()*1000)))
            async with self.session.get(a, headers={ "Referer": "http://play.afreecatv.com/{bjid}/null" }) as res:
                await res.text()
            async with self.session.get(f"http://play.afreecatv.com/{bjid}/null", headers={ "Referer": a, }) as res:
                html = await res.text()
        i, category = naive_parse(html, '"og:description" content="', " |")
        if i<=-1: 
            await self.close()
            print(html[:3000])
            print(bjid)
            raise Exception("cannot fetch category from html")
        i, bno = naive_parse(html, "nBroadNo = ", ";", i)
        if i<=-1:
            await self.close()
            raise Exception("cannot fetch broad number from html")
        if bno == "null":
            await self.close()
            raise DoNotStreaming
        else:
            self.is_streaming = True
        i, title = naive_parse(html, "szBroadTitle   = '", "'", i)
        if i<=-1: 
            await self.close()
            raise Exception("cannot fetch title from html")
        self.title = title
        i, started_at = naive_parse(html, "방송시작시간</strong><span>", "</", i)
        if i<=-1:
            await self.close()
            raise Exception("cannot fetch start time from html")
        self.started_at = datetime.datetime.strptime(started_at + "+0900", "%Y-%m-%d %H:%M:%S%z")
        self.bno = int(bno)
        headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Content-Type": "application/x-www-form-urlencoded",
            "Host": "live.afreecatv.com",
            "Origin": "http://play.afreecatv.com",
            "Referer": f"http://play.afreecatv.com/{bjid}/{bno}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
        }
        form = {
            "bid": bjid,
            "bno": self.bno,
            "type": "live",
            "pwd": '',
            "player_type": "html5",
            "stream_type": "common",
            "quality": "HD",
            "mode": "landing",
        }
        #async with self.session.post("http://live.afreecatv.com/afreeca/player_live_api.php?bjid=" + bjid, data=form, headers=headers) as res:
        async with self.session.post("http://live.afreecatv.com/afreeca/player_live_api.php?bjid=" + bjid, data=form, headers=headers) as res:
            metadata = json.loads(await res.text())["CHANNEL"]
        if metadata["RESULT"] == -2:
            self.is_streaming = False
            await self.close()
            raise ForignerDenied
        if metadata["RESULT"] == -6:
            self.is_streaming = False
            await self.close()
            raise AdultDenied
        if metadata["RESULT"] != 1:
            self.is_streaming = False
            await self.close()
            raise Exception("fail to fetch metadata" + ": " + bjid + ": "+ str(metadata))
        self.category = api.Game(
            id = int(metadata["CATE"]),
            name = category,
            box_art_url = None,
            )
        self.ws = await self.session.ws_connect(f"ws://218.38.31.169:10010/Websocket/" + bjid, protocols=("chat",), origin="http://play.afreecatv.com")
        packet = json.dumps({
            "SVC": "INIT",
            "RESULT": 0,
            "DATA": {
                "gate_ip": metadata["GWIP"],
                "gate_port": int(metadata["GWPT"]), 
                "center_ip": metadata["CTIP"], 
                "center_port": int(metadata["CTPT"]), 
                "broadno": self.bno,
                "cookie": "",
                "guid": gen_uid(),
                "cli_type": 41,
                "passwd": "",
                "category":  metadata["CATE"],
                "JOINLOG": "log\u0011\u0006&\u0006ad_uuid\u0006=\u0006ttjrFF4xZjgACVxM\u0006&\u0006uuid\u0006=\u00060xb374974eab9af73e\u0006&\u0006geo_cc\u0006=\u0006KR\u0006&\u0006geo_rc\u0006=\u000621\u0006&\u0006acpt_lang\u0006=\u0006ko_KR\u0006&\u0006svc_lang\u0006=\u0006ko_KR\u0006&\u0006os\u0006=\u0006win\u0006&\u0006is_streamer\u0006=\u0006false\u0006&\u0006path1\u0006=\u0006etc\u0012",
                "BJID": bjid,
                "fanticket": metadata["FTK"],
                "addinfo": "ad_langko",
                }
            }, separators=(',', ':'))
        await self.ws.send_str(packet)
        await self.ws.send_str('{"SVC":"GETSOLESVR","RESULT":0,"DATA":{"iType":1}}')
        return True
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
        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                msg = Message(msg.data, self.bjid)
                if msg.svc == "CLOSECH":
                    self.is_streaming = False
                    await self.close()
                    return msg
                elif msg.svc == "GETUSERCNTEX" or msg.svc == "GEUSERCNT":
                    self.main_pc_users = msg.main_pc_users
                    self.main_mobile_users = msg.main_mobile_users
                    self.sub_pc_users = msg.sub_pc_users
                    self.sub_mobile_users = msg.sub_mobile_users
                    self.join_pc_users = msg.join_pc_users
                    self.join_mobile_users = msg.join_mobile_users
                else:
                    pass
            elif msg.type == aiohttp.WSMsgType.ERROR:
                if self.retry > 0:
                    self.retry -= 1
                    await self.close()
                    await self.__init(self.bj_id, self.broad_no)
                    return await self.__anext__()
                else:
                    raise StopAsyncIteration
            if self.last_ping_time + self.PING_INTERVAL <= now:
                await self.ws.send_str('{"SVC":"KEEPALIVE","RESULT":0,"DATA":{}}')
                self.last_ping_time = now

async def test():
    c = await Client.connect("phonics1")
    print(c.is_streaming)
    async for msg in c:
        print(msg)

if __name__ == "__main__":
    asyncio.run(test())
