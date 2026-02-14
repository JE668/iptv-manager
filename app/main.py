import asyncio
import hashlib
import json
import os
import re
import time
import urllib.parse
import gzip
import logging
from datetime import datetime
from typing import Optional, List

import httpx
from fastapi import FastAPI, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Field, Session, SQLModel, create_engine, select
from sqlalchemy import text

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IPTV-Manager")

# --- 1. 数据库配置 ---
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

SQLModel.metadata.create_all(engine)

# 自动迁移
with engine.connect() as conn:
    try:
        inspector = conn.execute(text("PRAGMA table_info(source)"))
        columns = [row[1] for row in inspector]
        if "status" not in columns:
            conn.execute(text("ALTER TABLE source ADD COLUMN status VARCHAR DEFAULT 'Unknown'"))
        if "last_check" not in columns:
            conn.execute(text("ALTER TABLE source ADD COLUMN last_check FLOAT DEFAULT 0"))
        conn.commit()
    except Exception as e:
        logger.error(f"Database Migration Error: {e}")

# --- 2. 全局状态 ---
app = FastAPI()
templates = Jinja2Templates(directory="templates")
LOGO_BASE = "https://gcore.jsdelivr.net/gh/taksssss/tv/icon/"

class GlobalState:
    def __init__(self):
        self.active_streams = {}
        self.epg_content = b""
        self.is_checking = False

state = GlobalState()

# --- 3. 缓冲池逻辑 ---
class StreamPool:
    def __init__(self):
        self.streams = {}

    async def get_stream_info(self, url: str):
        try:
            cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-select_streams', 'v:0', '-analyzeduration', '3000000', url]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8.0)
            data = json.loads(stdout)
            if 'streams' in data and len(data['streams']) > 0:
                s = data['streams'][0]
                return {"res": f"{s.get('width')}x{s.get('height')}", "codec": s.get('codec_name', '未知').upper()}
        except: pass
        return {"res": "未知", "codec": "未知"}

    async def _fetcher(self, stream_id: str, url: str, name: str):
        retry_count = 0
        self.streams[stream_id] = {"name": name, "queues": [], "info": {"res": "探测中...", "codec": "探测中..."}, "speed": "0 KB/s"}
        
        while stream_id in self.streams:
            try:
                if retry_count == 0:
                    self.streams[stream_id]["info"] = await self.get_stream_info(url)
                
                async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=10.0), follow_redirects=True) as client:
                    async with client.stream("GET", url) as r:
                        if r.status_code != 200: raise Exception(f"HTTP {r.status_code}")
                        retry_count = 0
                        start_time = time.time()
                        bytes_count = 0
                        async for chunk in r.aiter_bytes(chunk_size=128*1024):
                            bytes_count += len(chunk)
                            now = time.time()
                            if now - start_time >= 1.0:
                                speed = (bytes_count / 1024) / (now - start_time)
                                if stream_id in self.streams:
                                    self.streams[stream_id]["speed"] = f"{speed:.1f} KB/s"
                                bytes_count, start_time = 0, now
                            if not self.streams[stream_id]["queues"]: return
                            for q in self.streams[stream_id]["queues"]:
                                try: q.put_nowait(chunk)
                                except asyncio.QueueFull: pass
            except Exception as e:
                retry_count += 1
                if stream_id in self.streams: self.streams[stream_id]["speed"] = f"重连中({retry_count})"
                if retry_count > 15: break
                await asyncio.sleep(2)
        self.streams.pop(stream_id, None)

    async def subscribe(self, name: str, url: str):
        stream_id = hashlib.md5(url.encode()).hexdigest()
        if stream_id not in self.streams:
            asyncio.create_task(self._fetcher(stream_id, url, name))
            await asyncio.sleep(0.5)
        queue = asyncio.Queue(maxsize=100)
        if stream_id in self.streams:
            self.streams[stream_id]["queues"].append(queue)
        try:
            while True:
                chunk = await queue.get()
                yield chunk
        finally:
            if stream_id in self.streams: self.streams[stream_id]["queues"].remove(queue)

stream_pool = StreamPool()

# --- 4. 后台维护逻辑 ---
async def run_maintenance():
    if state.is_checking: return
    state.is_checking = True
    logger.info("Starting maintenance task...")
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
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
        try:
            r = await client.get("https://epg.170909.xyz:1799/t.xml.gz")
            if r.status_code == 200:
                state.epg_content = gzip.decompress(r.content)
                logger.info("EPG Cached successfully.")
        except Exception as e:
            logger.error(f"EPG Update Failed: {e}")
    state.is_checking = False

async def maintenance_loop():
    while True:
        await run_maintenance()
        await asyncio.sleep(4 * 3600)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(maintenance_loop())

# --- 5. 路由 ---

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    try:
        with Session(engine) as session:
            sources = session.exec(select(Source)).all()
        return templates.TemplateResponse("index.html", {"request": request, "sources": sources})
    except Exception as e:
        return HTMLResponse(content=f"Error: {e}", status_code=500)

@app.get("/refresh")
async def manual_refresh():
    logger.info("Manual refresh triggered.")
    asyncio.create_task(run_maintenance())
    return RedirectResponse(url="/", status_code=303)

@app.get("/playlist.m3u")
async def get_m3u(request: Request, proxy: bool = False):
    try:
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
                    lines = resp.text.split('\n')
                    if s.type == 'm3u':
                        for i in range(len(lines)):
                            line = lines[i].strip()
                            if line.startswith("#EXTINF"):
                                name = line.split(',')[-1].strip()
                                # 寻找下一行非空的 URL
                                for j in range(i + 1, len(lines)):
                                    next_line = lines[j].strip()
                                    if next_line.startswith("http"):
                                        if next_line not in seen_urls:
                                            unique_channels.append({"name": name, "url": next_line, "group": "聚合频道"})
                                            seen_urls.add(next_line)
                                        break
                    else: # txt
                        for line in lines:
                            if ',' in line:
                                name, url = line.split(',', 1)
                                if url.strip() not in seen_urls:
                                    unique_channels.append({"name": name.strip(), "url": url.strip(), "group": "聚合频道"})
                                    seen_urls.add(url.strip())
                except Exception as e:
                    logger.warning(f"Failed to parse source {s.name}: {e}")
                    continue

        output = f'#EXTM3U x-tvg-url="{base_url}/epg.xml"\n'
        for c in unique_channels:
            logo = f"{LOGO_BASE}{c['name']}.png"
            final_url = f"{base_url}/live/{c['name']}?url={urllib.parse.quote(c['url'], safe='')}" if proxy else c['url']
            output += f'#EXTINF:-1 tvg-name="{c["name"]}" tvg-logo="{logo}" group-title="{c["group"]}",{c["name"]}\n{final_url}\n'
        return Response(content=output, media_type="application/x-mpegurl")
    except Exception as e:
        logger.error(f"M3U Generation Error: {e}")
        return Response(content=f"Internal Error: {e}", status_code=500)

@app.get("/playlist.txt")
async def get_txt(request: Request, proxy: bool = False):
    # 复用逻辑... (简化处理)
    resp = await get_m3u(request, proxy)
    if resp.status_code != 200: return resp
    lines = []
    # 从 M3U 转回 TXT
    m3u_text = resp.body.decode()
    items = re.findall(r'#EXTINF:.*?,(.*?)\n(http.*)', m3u_text)
    for name, url in items:
        lines.append(f"{name},{url}")
    return Response(content="\n".join(lines), media_type="text/plain")

@app.get("/live/{channel_name}")
async def proxy_live(channel_name: str, url: str):
    return StreamingResponse(stream_pool.subscribe(channel_name, url), media_type="video/mp2t")

@app.get("/epg.xml")
async def get_epg():
    if not state.epg_content:
        return Response(content='<?xml version="1.0" encoding="UTF-8"?><tv></tv>', media_type="application/xml")
    return Response(content=state.epg_content, media_type="application/xml")

@app.get("/api/status")
async def get_api_status():
    return {
        "active_streams": [
            {"name": v["name"], "clients": len(v["queues"]), "speed": v["speed"], "info": v["info"]}
            for v in stream_pool.streams.values()
        ],
        "is_checking": state.is_checking
    }

@app.post("/add_source")
async def add_source(name: str = Form(...), url: str = Form(...), type: str = Form(...)):
    with Session(engine) as session:
        session.add(Source(name=name, url=url, type=type))
        session.commit()
    asyncio.create_task(run_maintenance())
    return RedirectResponse(url="/", status_code=303)

@app.get("/delete/{source_id}")
async def delete_source(source_id: int):
    with Session(engine) as session:
        source = session.get(Source, source_id)
        if source:
            session.delete(source)
            session.commit()
    return RedirectResponse(url="/", status_code=303)
