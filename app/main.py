import asyncio
import hashlib
import json
import os
import re
import time
import urllib.parse
import gzip
from datetime import datetime
from typing import Optional, List

import httpx
from fastapi import FastAPI, Form, Request, Response, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Field, Session, SQLModel, create_engine, select
from sqlalchemy import text

# --- 1. 数据库配置与模型 ---
DB_FILE = "/app/data/iptv.db"
os.makedirs("/app/data", exist_ok=True)
engine = create_engine(f"sqlite:///{DB_FILE}", connect_args={"check_same_thread": False})

class Source(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    url: str
    type: str  # m3u 或 txt
    status: str = "Unknown"
    last_check: float = 0

# 初始化表结构
SQLModel.metadata.create_all(engine)

# 自动迁移逻辑：确保旧数据库也能增加新列
with engine.connect() as conn:
    inspector = conn.execute(text("PRAGMA table_info(source)"))
    columns = [row[1] for row in inspector]
    if "status" not in columns:
        conn.execute(text("ALTER TABLE source ADD COLUMN status VARCHAR DEFAULT 'Unknown'"))
    if "last_check" not in columns:
        conn.execute(text("ALTER TABLE source ADD COLUMN last_check FLOAT DEFAULT 0"))
    conn.commit()

# --- 2. 全局状态与配置 ---
app = FastAPI()
templates = Jinja2Templates(directory="templates")
LOGO_BASE = "https://gcore.jsdelivr.net/gh/taksssss/tv/icon/"

class GlobalState:
    def __init__(self):
        self.active_streams = {}  # 实时缓冲池：{stream_id: {data}}
        self.epg_content = b""    # 缓存解压后的 EPG 内容
        self.is_checking = False

state = GlobalState()

# --- 3. 核心功能：流媒体缓冲池 (一拉多 & 自愈) ---
class StreamPool:
    def __init__(self):
        self.streams = {}

    async def get_stream_info(self, url: str):
        """利用 ffprobe 探测分辨率和编码"""
        try:
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_streams', '-select_streams', 'v:0',
                '-analyzeduration', '3000000', '-probesize', '3000000', url
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10.0)
            data = json.loads(stdout)
            if 'streams' in data and len(data['streams']) > 0:
                s = data['streams'][0]
                return {
                    "res": f"{s.get('width')}x{s.get('height')}",
                    "codec": s.get('codec_name', '未知').upper()
                }
        except:
            pass
        return {"res": "未知", "codec": "未知"}

    async def _fetcher(self, stream_id: str, url: str, name: str):
        """单例拉流协程：负责向上游请求并分发给所有订阅者队列"""
        retry_count = 0
        self.streams[stream_id] = {
            "name": name,
            "url": url,
            "queues": [],
            "info": {"res": "探测中...", "codec": "探测中..."},
            "speed": "0 KB/s",
            "start_time": time.time()
        }

        while stream_id in self.streams:
            try:
                # 仅在第一次连接时探测信息
                if retry_count == 0:
                    self.streams[stream_id]["info"] = await self.get_stream_info(url)

                timeout = httpx.Timeout(10.0, read=10.0)
                async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                    async with client.stream("GET", url) as r:
                        if r.status_code != 200:
                            raise Exception(f"HTTP {r.status_code}")
                        
                        retry_count = 0
                        start_time = time.time()
                        bytes_count = 0
                        
                        async for chunk in r.aiter_bytes(chunk_size=128*1024):
                            bytes_count += len(chunk)
                            # 计算网速
                            now = time.time()
                            if now - start_time >= 1.0:
                                speed = (bytes_count / 1024) / (now - start_time)
                                if stream_id in self.streams:
                                    self.streams[stream_id]["speed"] = f"{speed:.1f} KB/s"
                                bytes_count = 0
                                start_time = now

                            # 检查是否还有人在看
                            if not self.streams[stream_id]["queues"]:
                                return

                            # 分发到所有下游队列
                            for q in self.streams[stream_id]["queues"]:
                                try:
                                    q.put_nowait(chunk)
                                except asyncio.QueueFull:
                                    pass
            except Exception as e:
                retry_count += 1
                if stream_id in self.streams:
                    self.streams[stream_id]["speed"] = f"重连中({retry_count})"
                if retry_count > 20: break
                await asyncio.sleep(2)

        self.streams.pop(stream_id, None)

    async def subscribe(self, name: str, url: str):
        """下游订阅入口"""
        stream_id = hashlib.md5(url.encode()).hexdigest()
        
        if stream_id not in self.streams:
            asyncio.create_task(self._fetcher(stream_id, url, name))
            await asyncio.sleep(0.5)  # 等待初始化

        queue = asyncio.Queue(maxsize=100)
        if stream_id in self.streams:
            self.streams[stream_id]["queues"].append(queue)
        
        try:
            while True:
                chunk = await queue.get()
                yield chunk
        finally:
            if stream_id in self.streams:
                self.streams[stream_id]["queues"].remove(queue)

stream_pool = StreamPool()

# --- 4. 定时维护任务：源检测与 EPG ---
async def run_maintenance():
    if state.is_checking: return
    state.is_checking = True
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        # 1. 检测所有订阅源存活情况
        with Session(engine) as session:
            sources = session.exec(select(Source)).all()
            for s in sources:
                try:
                    r = await client.get(s.url, headers={"Range": "bytes=0-100"})
                    s.status = "Online" if r.status_code < 400 else "Offline"
                except: s.status = "Offline"
                s.last_check = time.time()
                session.add(s)
            session.commit()
        # 2. 缓存 EPG
        try:
            epg_url = "https://epg.170909.xyz:1799/t.xml.gz"
            r = await client.get(epg_url)
            if r.status_code == 200:
                state.epg_content = gzip.decompress(r.content)
        except: pass
    state.is_checking = False

async def maintenance_loop():
    while True:
        await run_maintenance()
        await asyncio.sleep(4 * 3600)  # 4小时巡检一次

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(maintenance_loop())

# --- 5. 路由逻辑 ---

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
    return templates.TemplateResponse("index.html", {"request": request, "sources": sources})

@app.get("/playlist.m3u")
@app.get("/playlist.txt")
async def get_playlist(request: Request, proxy: bool = False):
    """
    聚合 M3U/TXT 核心逻辑
    - URL 去重：相同 URL 仅保留一个，不同 URL 同名保留。
    - 协议自适应：自动处理 HTTPS 反向代理。
    """
    # 处理协议和域名
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", str(request.base_url.netloc))
    base_url = f"{scheme}://{host}"

    unique_channels = []
    seen_urls = set()
    
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()

    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        for s in sources:
            try:
                resp = await client.get(s.url)
                if s.type == 'm3u':
                    # 匹配 #EXTINF 和下一行的 URL
                    items = re.findall(r'#EXTINF:-1.*?(?:group-title="(.*?)")?.*?,(.*?)\n(http.*)', resp.text)
                    for group, name, url in items:
                        u = url.strip()
                        if u not in seen_urls:
                            unique_channels.append({"name": name.strip(), "url": u, "group": group or "聚合频道"})
                            seen_urls.add(u)
                else:
                    for line in resp.text.split('\n'):
                        if ',' in line:
                            name, url = line.split(',', 1)
                            u = url.strip()
                            if u not in seen_urls:
                                unique_channels.append({"name": name.strip(), "url": u, "group": "聚合频道"})
                                seen_urls.add(u)
            except: continue

    # 输出格式判断
    is_txt
