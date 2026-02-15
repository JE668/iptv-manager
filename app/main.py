import asyncio
import hashlib
import json
import os
import re
import time
import urllib.parse
import gzip
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict

import httpx
from fastapi import FastAPI, Form, Request, Response, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Field, Session, SQLModel, create_engine, select
from sqlalchemy import text
from lxml import etree

# --- 1. 初始化 ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IPTV-Manager")

ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin123")
SECRET_KEY = hashlib.sha256(ADMIN_PASS.encode()).hexdigest()

DB_FILE = "/app/data/iptv.db"
os.makedirs("/app/data", exist_ok=True)
engine = create_engine(f"sqlite:///{DB_FILE}", connect_args={"check_same_thread": False})

class Source(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    url: str
    type: str 
    status: str = "Unknown"
    last_check: float = 0

class EPGSource(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    url: str
    status: str = "Unknown"

class Setting(SQLModel, table=True):
    key: str = Field(primary_key=True)
    value: str

SQLModel.metadata.create_all(engine)

with Session(engine) as session:
    if not session.get(Setting, "epg_days"): session.add(Setting(key="epg_days", value="3"))
    if not session.get(Setting, "proxy_mode"): session.add(Setting(key="proxy_mode", value="1"))
    if not session.get(Setting, "epg_interval"): session.add(Setting(key="epg_interval", value="6"))
    session.commit()

app = FastAPI()
templates = Jinja2Templates(directory="templates")
LOGO_BASE = "https://gcore.jsdelivr.net/gh/taksssss/tv/icon/"

class GlobalState:
    def __init__(self):
        self.epg_xml = b""
        self.epg_gz = b""
        self.last_epg_update = 0
        self.last_playlist_update = 0
        self.is_epg_updating = False
        self.epg_logs = []

state = GlobalState()

def is_authenticated(request: Request):
    return request.cookies.get("session_id") == SECRET_KEY

# --- 2. 核心流控引擎 ---

class StreamPool:
    def __init__(self):
        self.streams: Dict = {}

    async def get_stream_info(self, url: str):
        try:
            # 增加探测时长，防止慢源识别不出
            cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-select_streams', 'v:0', '-analyzeduration', '5000000', url]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=12.0)
            data = json.loads(stdout)
            if 'streams' in data and len(data['streams']) > 0:
                s = data['streams'][0]
                return {"res": f"{s.get('width')}x{s.get('height')}", "codec": s.get('codec_name', '未知').upper()}
        except Exception as e:
            logger.warning(f"ffprobe 探测失败: {e}")
        return {"res": "未知", "codec": "未知"}

    async def _fetcher(self, stream_id: str, url: str, name: str):
        retry_count = 0
        logger.info(f"🚀 [中继启动] {name}")
        
        while stream_id in self.streams:
            if not self.streams[stream_id]["clients"]:
                logger.info(f"👋 [观众离开] 停止中继: {name}")
                break
            
            try:
                # 每轮重连都更新一下探测信息（如果还没探测到）
                if self.streams[stream_id]["info"]["res"] == "探测中...":
                    self.streams[stream_id]["info"] = await self.get_stream_info(url)

                # 使用较长的连接和读取超时，适配弱网源
                timeout = httpx.Timeout(15.0, read=15.0, connect=15.0)
                async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                    async with client.stream("GET", url) as r:
                        if r.status_code != 200:
                            raise Exception(f"HTTP ERROR {r.status_code}")
                        
                        retry_count = 0
                        start_time, bytes_in = time.time(), 0
                        async for chunk in r.aiter_bytes(chunk_size=128*1024):
                            if not self.streams[stream_id]["clients"]: return
                            
                            bytes_in += len(chunk)
                            now = time.time()
                            if now - start_time >= 1.0:
                                self.streams[stream_id]["in_speed"] = f"{(bytes_in / 1024 / (now-start_time)):.1f} KB/s"
                                bytes_in, start_time = 0, now
                            
                            for q in self.streams[stream_id]["queues"]:
                                try: q.put_nowait(chunk)
                                except asyncio.QueueFull: pass
            except Exception as e:
                retry_count += 1
                logger.error(f"❌ [中继故障] {name}: {e} (重试 {retry_count})")
                if stream_id in self.streams:
                    self.streams[stream_id]["in_speed"] = f"重连({retry_count})"
                if retry_count > 10: break
                await asyncio.sleep(3)
        
        self.streams.pop(stream_id, None)

    async def subscribe(self, name: str, url: str, client_ip: str):
        stream_id = hashlib.md5(url.encode()).hexdigest()
        client_id = hashlib.md5(f"{client_ip}{time.time()}".encode()).hexdigest()[:8]
        
        if stream_id not in self.streams:
            self.streams[stream_id] = {
                "name": name, "url": url, "queues": [], "clients": {}, 
                "info": {"res": "探测中...", "codec": "探测中..."}, 
                "in_speed": "0 KB/s", "out_speed": "0 KB/s", "buffer_level": 0
            }
            asyncio.create_task(self._fetcher(stream_id, url, name))
            await asyncio.sleep(0.3) # 给 fetcher 一点启动时间

        queue = asyncio.Queue(maxsize=100)
        if stream_id in self.streams:
            self.streams[stream_id]["queues"].append(queue)
            self.streams[stream_id]["clients"][client_id] = {"ip": client_ip, "out_bytes": 0, "speed": "0 KB/s", "last_ts": time.time()}
        
        try:
            while True:
                chunk = await queue.get()
                if stream_id not in self.streams: break
                
                # 计算下游流量
                c = self.streams[stream_id]["clients"].get(client_id)
                if c:
                    c["out_bytes"] += len(chunk)
                    now = time.time()
                    if now - c["last_ts"] >= 1.0:
                        c["speed"] = f"{(c['out_bytes'] / 1024 / (now-c['last_ts'])):.1f} KB/s"
                        c["out_bytes"], c["last_ts"] = 0, now
                        # 计算总下游
                        total_out = sum([float(cli["speed"].split(' ')[0]) for cli in self.streams[stream_id]["clients"].values()])
                        self.streams[stream_id]["out_speed"] = f"{total_out:.1f} KB/s"
                
                self.streams[stream_id]["buffer_level"] = queue.qsize()
                yield chunk
        finally:
            if stream_id in self.streams:
                self.streams[stream_id]["queues"].remove(queue)
                self.streams[stream_id]["clients"].pop(client_id, None)

stream_pool = StreamPool()

# --- 3. 维护与 EPG ---

async def update_epg_task():
    if state.is_epg_updating: return
    state.is_epg_updating = True
    # 使用本地时间显示日志
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    state.epg_logs.insert(0, f"[{ts}] ⏳ 启动聚合任务...")
    
    master_root = etree.Element("tv")
    with Session(engine) as session:
        sources = session.exec(select(EPGSource)).all()
        days_limit = int(session.get(Setting, "epg_days").value)
    
    cutoff = datetime.now() - timedelta(days=1)
    end = datetime.now() + timedelta(days=days_limit)
    
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        for s in sources:
            try:
                r = await client.get(s.url)
                content = r.content
                if s.url.endswith(".gz") or content[:2] == b'\x1f\x8b': content = gzip.decompress(content)
                root = etree.fromstring(content, parser=etree.XMLParser(recover=True))
                channels = root.xpath("//channel"); progs = root.xpath("//programme")
                v_progs = 0
                for c in channels: master_root.append(c)
                for p in progs:
                    try:
                        st = datetime.strptime(p.get("start")[:14], "%Y%m%d%H%M%S")
                        if cutoff <= st <= end: master_root.append(p); v_progs += 1
                    except: pass
                s.status = "Success"
                state.epg_logs.insert(0, f"[{ts}] ✅ {s.name}: 导入 {len(channels)} 频道, {v_progs} 节目")
            except Exception as e:
                s.status = "Error"
                state.epg_logs.insert(0, f"[{ts}] ❌ {s.name}: {str(e)}")
            with Session(engine) as session: session.add(s); session.commit()
    
    final = etree.tostring(master_root, encoding="UTF-8", xml_declaration=True, pretty_print=True)
    state.epg_xml, state.epg_gz = final, gzip.compress(final)
    state.last_epg_update, state.is_epg_updating = time.time(), False

@app.on_event("startup")
async def startup():
    asyncio.create_task(update_epg_task())
    # 启动 EPG 定时器
    async def epg_timer():
        while True:
            await asyncio.sleep(3600 * 6)
            await update_epg_task()
    asyncio.create_task(epg_timer())

# --- 4. 实时订阅获取 ---

async def fetch_realtime_sources(force_proxy: bool = False):
    unique_channels, seen_urls = [], set()
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
        p_mode = int(session.get(Setting, "proxy_mode").value)
    
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
        # 并行抓取提升响应速度
        tasks = [client.get(s.url) for s in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, resp in enumerate(results):
            if isinstance(resp, Exception): continue
            lines = resp.text.split('\n')
            for j in range(len(lines)):
                line = lines[j].strip()
                if line.startswith("#EXTINF"):
                    name = line.split(',')[-1].strip()
                    for k in range(j+1, min(j+5, len(lines))):
                        u = lines[k].strip()
                        if u.startswith("http") and u not in seen_urls:
                            # 判定走代理逻辑：force_proxy 拥有最高优先级
                            use_proxy = force_proxy or (p_mode == 2) or (p_mode == 1 and ("/udp/" in u or "/rtp/" in u))
                            unique_channels.append({"name": name, "url": u, "use_proxy": use_proxy})
                            seen_urls.add(u); break
                elif ',' in line and not line.startswith("#"):
                    parts = line.split(',', 1)
                    name, u = parts[0].strip(), parts[1].strip()
                    if u.startswith("http") and u not in seen_urls:
                        use_proxy = force_proxy or (p_mode == 2) or (p_mode == 1 and ("/udp/" in u or "/rtp/" in u))
                        unique_channels.append({"name": name, "url": u, "use_proxy": use_proxy})
                        seen_urls.add(u)
    return unique_channels

# --- 5. 路由 ---

@app.get("/playlist.m3u")
@app.get("/playlist.txt")
async def get_playlist(request: Request, proxy: bool = False):
    scheme = request.headers.get("x-f
