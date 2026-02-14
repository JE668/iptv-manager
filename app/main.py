from fastapi import FastAPI, Request, Form, Response, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import SQLModel, Field, Session, select, create_engine
from sqlalchemy import text
import httpx
import re
import os
import asyncio
import time
import json
import urllib.parse
import hashlib
import gzip
from datetime import datetime

# --- 1. 数据库与初始化 ---
DB_FILE = "/app/data/iptv.db"
os.makedirs("/app/data", exist_ok=True)
engine = create_engine(f"sqlite:///{DB_FILE}", connect_args={"check_same_thread": False})

class Source(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    url: str
    type: str # m3u 或 txt
    status: str = "Unknown"
    last_check: float = 0

SQLModel.metadata.create_all(engine)

# 自动迁移旧数据库
with engine.connect() as conn:
    inspector = conn.execute(text("PRAGMA table_info(source)"))
    columns = [row[1] for row in inspector]
    if "status" not in columns:
        conn.execute(text("ALTER TABLE source ADD COLUMN status VARCHAR DEFAULT 'Unknown'"))
    if "last_check" not in columns:
        conn.execute(text("ALTER TABLE source ADD COLUMN last_check FLOAT DEFAULT 0"))
    conn.commit()

app = FastAPI()
templates = Jinja2Templates(directory="templates")

class GlobalState:
    def __init__(self):
        self.active_streams = {}
        self.epg_content = b"" # 存储解压后的 EPG 内容
        self.is_checking = False

state = GlobalState()

# --- 2. 核心逻辑：源检测与 EPG 下载 ---

async def run_maintenance():
    """核心维护任务：检测源 + 更新 EPG"""
    if state.is_checking: return
    state.is_checking = True
    print(f"[{datetime.now()}] 🔄 开始后台维护任务...")
    
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        # 1. 检测所有订阅源
        with Session(engine) as session:
            sources = session.exec(select(Source)).all()
            for s in sources:
                try:
                    # 使用 GET 请求前几个字节来判断源是否存活
                    r = await client.get(s.url, follow_redirects=True, headers={"Range": "bytes=0-100"})
                    s.status = "Online" if r.status_code < 400 else "Offline"
                except:
                    s.status = "Offline"
                s.last_check = time.time()
                session.add(s)
            session.commit()

        # 2. 更新 EPG (Requirement 4)
        try:
            epg_url = "https://epg.170909.xyz:1799/t.xml.gz"
            r = await client.get(epg_url)
            if r.status_code == 200:
                state.epg_content = gzip.decompress(r.content)
                print("✅ EPG 已下载并本地缓存")
        except Exception as e:
            print(f"❌ EPG 更新失败: {e}")

    state.is_checking = False
    print("✨ 维护任务完成")

@app.on_event("startup")
async def startup_event():
    # 启动时立即运行一次检测
    asyncio.create_task(run_maintenance())

# --- 3. 路由设置 ---

@app.get("/refresh")
async def manual_refresh():
    """手动触发检测的接口"""
    asyncio.create_task(run_maintenance())
    return RedirectResponse(url="/", status_code=303)

@app.get("/epg.xml")
async def get_epg():
    """提供本地缓存的 EPG (Requirement 4)"""
    if not state.epg_content:
        return Response(content="EPG not ready", status_code=503)
    return Response(content=state.epg_content, media_type="application/xml")

# --- 此处复用之前的 /playlist.m3u, /live/{name}, /api/status 等逻辑 ---
# (为了篇幅，重点展示变更部分，确保你的代码中保留了之前的 StreamPool 和 subscribe 逻辑)

# [请确保此处保留之前完善的 StreamPool 类及其方法]
# ... 之前的 StreamPool 代码 ...
class StreamPool:
    def __init__(self):
        self.streams = {}

    async def get_stream_info(self, url):
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

    async def _fetcher(self, stream_id, url, name):
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
                        async for chunk in r.aiter_bytes(chunk_size=128*1024):
                            if not self.streams[stream_id]["queues"]: return
                            for q in self.streams[stream_id]["queues"]:
                                try: q.put_nowait(chunk)
                                except asyncio.QueueFull: pass
            except:
                retry_count += 1
                if stream_id in self.streams: self.streams[stream_id]["speed"] = f"重连中({retry_count})"
                if retry_count > 15: break
                await asyncio.sleep(2)
        self.streams.pop(stream_id, None)

    async def subscribe(self, name, url):
        stream_id = hashlib.md5(url.encode()).hexdigest()
        if stream_id not in self.streams:
            asyncio.create_task(self._fetcher(stream_id, url, name))
            await asyncio.sleep(0.5)
        queue = asyncio.Queue(maxsize=100)
        self.streams[stream_id]["queues"].append(queue)
        try:
            while True:
                chunk = await queue.get()
                yield chunk
        finally:
            if stream_id in self.streams: self.streams[stream_id]["queues"].remove(queue)

stream_pool = StreamPool()

@app.get("/live/{channel_name}")
async def proxy_live(channel_name: str, url: str):
    return StreamingResponse(stream_pool.subscribe(channel_name, url), media_type="video/mp2t")

@app.get("/api/status")
async def get_status():
    return {
        "active_streams": [
            {"name": v["name"], "clients": len(v["queues"]), "speed": v["speed"], "info": v["info"]}
            for v in stream_pool.streams.values()
        ],
        "is_checking": state.is_checking
    }

@app.get("/playlist.m3u")
async def get_m3u(request: Request, proxy: bool = False):
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
                    items = re.findall(r'#EXTINF:-1.*?(?:group-title="(.*?)")?.*?,(.*?)\n(http.*)', resp.text)
                    for group, name, url in items:
                        u = url.strip()
                        if u not in seen_urls:
                            unique_channels.append({"name": name.strip(), "url": u, "group": group or "未分类"})
                            seen_urls.add(u)
                else:
                    for line in resp.text.split('\n'):
                        if ',' in line:
                            n, u = line.split(',', 1)
                            if u.strip() not in seen_urls:
                                unique_channels.append({"name": n.strip(), "url": u.strip(), "group": "未分类"})
                                seen_urls.add(u.strip())
            except: continue

    output = f'#EXTM3U x-tvg-url="{base_url}/epg.xml"\n'
    for c in unique_channels:
        logo = f"https://gcore.jsdelivr.net/gh/taksssss/tv/icon/{c['name']}.png"
        final_url = f"{base_url}/live/{c['name']}?url={urllib.parse.quote(c['url'], safe='')}" if proxy else c['url']
        output += f'#EXTINF:-1 tvg-name="{c["name"]}" tvg-logo="{logo}" group-title="{c["group"]}",{c["name"]}\n{final_url}\n'
    return Response(content=output, media_type="application/x-mpegurl")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
    return templates.TemplateResponse("index.html", {"request": request, "sources": sources})

@app.post("/add_source")
async def add_source(name: str = Form(...), url: str = Form(...), type: str = Form(...)):
    with Session(engine) as session:
        session.add(Source(name=name, url=url, type=type))
        session.commit()
    return RedirectResponse(url="/refresh", status_code=303)

@app.get("/delete/{source_id}")
async def delete_source(source_id: int):
    with Session(engine) as session:
        source = session.get(Source, source_id)
        if source: session.delete(source)
        session.commit()
    return RedirectResponse(url="/", status_code=303)
