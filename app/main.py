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
from fastapi import FastAPI, Form, Request, Response, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Field, Session, SQLModel, create_engine, select
from sqlalchemy import text

# --- 1. 配置与日志 ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IPTV-Manager")

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

# 初始化数据库并自动迁移
SQLModel.metadata.create_all(engine)
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

app = FastAPI()
templates = Jinja2Templates(directory="templates")
LOGO_BASE = "https://gcore.jsdelivr.net/gh/taksssss/tv/icon/"

class GlobalState:
    def __init__(self):
        self.epg_content = b""
        self.is_checking = False

state = GlobalState()

# --- 2. 频道清洗与分组工具 ---

def clean_channel_name(name: str) -> str:
    """清理频道名称中的冗余词汇"""
    name = name.upper().replace(" ", "")
    # 移除常见干扰词
    patterns = [
        r"\(.*\)", r"（.*）", r"HD", r"高清", r"超清", r"4K", r"8K", r"蓝光", 
        r"V2", r"V3", r"\(备用\)", r"\[.*\]", r"频道", r"-"
    ]
    for p in patterns:
        name = re.sub(p, "", name)
    # 规范化 CCTV
    name = re.sub(r"CCTV(\d+)\+", r"CCTV\1+", name)
    return name.strip()

def get_auto_group(name: str, original_group: str = "") -> str:
    """根据名称自动识别分组"""
    if original_group and original_group not in ["", "未分类", "聚合频道"]:
        return original_group
    
    name = name.upper()
    if "CCTV" in name: return "央视频道"
    if "卫视" in name: return "卫视频道"
    if any(x in name for x in ["电影", "影院", "HBO", "剧场"]): return "电影频道"
    if any(x in name for x in ["体育", "足球", "篮球", "NBA"]): return "体育频道"
    if any(x in name for x in ["少儿", "卡通", "动漫"]): return "少儿频道"
    if any(x in name for x in ["新闻", "资讯"]): return "新闻频道"
    if any(x in name for x in ["购物", "商城"]): return "购物频道"
    return "地方频道"

# --- 3. 缓冲池逻辑 (一拉多 & 自愈) ---

class StreamPool:
    def __init__(self):
        self.streams = {} # {stream_id: {data}}

    async def get_stream_info(self, url: str):
        try:
            cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-select_streams', 'v:0', '-analyzeduration', '3000000', url]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10.0)
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
                
                timeout = httpx.Timeout(10.0, read=10.0)
                async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
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
            except Exception:
                retry_count += 1
                if stream_id in self.streams: self.streams[stream_id]["speed"] = f"重连中({retry_count})"
                if retry_count > 20: break
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
            if stream_id in self.streams:
                self.streams[stream_id]["queues"].remove(queue)

stream_pool = StreamPool()

# --- 4. 聚合维护任务 ---

async def run_maintenance():
    if state.is_checking: return
    state.is_checking = True
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
        except: pass
    state.is_checking = False

async def maintenance_loop():
    while True:
        await run_maintenance()
        await asyncio.sleep(4 * 3600)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(maintenance_loop())

# --- 5. 核心路由 ---

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
    return templates.TemplateResponse("index.html", {"request": request, "sources": sources})

@app.get("/playlist.m3u")
@app.get("/playlist.txt")
async def get_playlist(request: Request, proxy: bool = False):
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", str(request.base_url.netloc))
    base_url = f"{scheme}://{host}"
    
    unique_channels = []
    seen_urls = set()
    
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for s in sources:
            try:
                resp = await client.get(s.url)
                lines = resp.text.split('\n')
                if s.type == 'm3u':
                    for i in range(len(lines)):
                        line = lines[i].strip()
                        if line.startswith("#EXTINF"):
                            group_match = re.search(r'group-title="(.*?)"', line)
                            original_group = group_match.group(1) if group_match else ""
                            raw_name = line.split(',')[-1].strip()
                            clean_name = clean_channel_name(raw_name)
                            for j in range(i + 1, min(i + 5, len(lines))):
                                u = lines[j].strip()
                                if u.startswith("http"):
                                    if u not in seen_urls:
                                        unique_channels.append({"name": clean_name, "url": u, "group": get_auto_group(clean_name, original_group)})
                                        seen_urls.add(u)
                                    break
                else:
                    for line in lines:
                        if ',' in line:
                            raw_name, url = line.split(',', 1)
                            clean_name = clean_channel_name(raw_name.strip())
                            u = url.strip()
                            if u not in seen_urls:
                                unique_channels.append({"name": clean_name, "url": u, "group": get_auto_group(clean_name)})
                                seen_urls.add(u)
            except: continue

    unique_channels.sort(key=lambda x: (x['group'], x['name']))
    is_txt = request.url.path.endswith('.txt')

    if is_txt:
        lines, current_group = [], ""
        for c in unique_channels:
            if c['group'] != current_group:
                current_group = c['group']
                lines.append(f"{current_group},#genre#")
            final_url = f"{base_url}/live/{c['name']}?url={urllib.parse.quote(c['url'], safe='')}" if proxy else c['url']
            lines.append(f"{c['name']},{final_url}")
        return Response(content="\n".join(lines), media_type="text/plain")
    else:
        output = f'#EXTM3U x-tvg-url="{base_url}/epg.xml"\n'
        for c in unique_channels:
            logo = f"{LOGO_BASE}{c['name']}.png"
            final_url = f"{base_url}/live/{c['name']}?url={urllib.parse.quote(c['url'], safe='')}" if proxy else c['url']
            output += f'#EXTINF:-1 tvg-name="{c["name"]}" tvg-logo="{logo}" group-title="{c["group"]}",{c["name"]}\n{final_url}\n'
        return Response(content=output, media_type="application/x-mpegurl")

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
        "active_streams": [{"name": v["name"], "clients": len(v["queues"]), "speed": v["speed"], "info": v["info"]} for v in stream_pool.streams.values()],
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
        if source: session.delete(source)
        session.commit()
    return RedirectResponse(url="/", status_code=303)

@app.get("/refresh")
async def manual_refresh():
    asyncio.create_task(run_maintenance())
    return RedirectResponse(url="/", status_code=303)
