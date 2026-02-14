from fastapi import FastAPI, Request, Form, Response, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import SQLModel, Field, Session, select, create_engine
import httpx
import re
import os
import asyncio
import time
import json
import urllib.parse
import hashlib
import gzip

# --- 1. 数据库模型扩展 ---
DB_FILE = "/app/data/iptv.db"
os.makedirs("/app/data", exist_ok=True)
engine = create_engine(f"sqlite:///{DB_FILE}")

class Source(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    url: str
    type: str # m3u 或 txt
    status: str = "Unknown" # Online / Offline / Unknown
    last_check: float = 0

SQLModel.metadata.create_all(engine)
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# --- 2. 全局状态存储 ---
class GlobalState:
    def __init__(self):
        self.active_streams = {} # 实时缓冲池
        self.epg_data = '<?xml version="1.0" encoding="UTF-8"?><tv></tv>'
        self.source_channels = [] # 聚合后的频道列表缓存

state = GlobalState()

# --- 3. 核心功能：流媒体缓冲池 (保持之前的自愈逻辑) ---
class StreamPool:
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
        state.active_streams[stream_id] = {"name": name, "queues": [], "info": {"res": "探测中...", "codec": "探测中..."}, "speed": "0 KB/s"}
        
        while stream_id in state.active_streams:
            try:
                if retry_count == 0:
                    state.active_streams[stream_id]["info"] = await self.get_stream_info(url)
                
                async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=10.0), follow_redirects=True) as client:
                    async with client.stream("GET", url) as r:
                        if r.status_code != 200: raise Exception(f"HTTP {r.status_code}")
                        retry_count = 0
                        async for chunk in r.aiter_bytes(chunk_size=128*1024):
                            if not state.active_streams[stream_id]["queues"]: return
                            for q in state.active_streams[stream_id]["queues"]:
                                try: q.put_nowait(chunk)
                                except asyncio.QueueFull: pass
            except Exception as e:
                retry_count += 1
                state.active_streams[stream_id]["speed"] = f"重连中({retry_count})"
                if retry_count > 15: break
                await asyncio.sleep(2)
        state.active_streams.pop(stream_id, None)

    async def subscribe(self, name, url):
        stream_id = hashlib.md5(url.encode()).hexdigest()
        if stream_id not in state.active_streams:
            asyncio.create_task(self._fetcher(stream_id, url, name))
            await asyncio.sleep(0.5) # 等待任务初始化
        
        queue = asyncio.Queue(maxsize=100)
        state.active_streams[stream_id]["queues"].append(queue)
        try:
            while True:
                chunk = await queue.get()
                yield chunk
        finally:
            if stream_id in state.active_streams:
                state.active_streams[stream_id]["queues"].remove(queue)

pool = StreamPool()

# --- 4. 定时任务：EPG 聚合与源检测 ---

async def update_tasks():
    """每 4 小时运行一次：更新 EPG，检查源存活"""
    while True:
        print("⏰ 开始执行后台维护任务...")
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            # 1. 检查订阅源存活状态
            with Session(engine) as session:
                sources = session.exec(select(Source)).all()
                for s in sources:
                    try:
                        r = await client.head(s.url)
                        s.status = "Online" if r.status_code < 400 else "Offline"
                    except: s.status = "Offline"
                    s.last_check = time.time()
                    session.add(s)
                session.commit()

            # 2. 抓取并缓存 EPG (此处以你要求的源为例)
            try:
                epg_url = "https://epg.170909.xyz:1799/t.xml.gz"
                r = await client.get(epg_url)
                if r.status_code == 200:
                    state.epg_data = gzip.decompress(r.content).decode('utf-8')
                    print("✅ EPG 更新成功")
            except Exception as e:
                print(f"❌ EPG 更新失败: {e}")

        await asyncio.sleep(4 * 3600)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_tasks())

# --- 5. 路由逻辑 ---

@app.get("/epg.xml")
async def get_local_epg():
    return Response(content=state.epg_data, media_type="application/xml")

@app.get("/playlist.m3u")
async def get_m3u(request: Request, proxy: bool = False):
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", str(request.base_url.netloc))
    base_url = f"{scheme}://{host}"
    
    # 聚合逻辑
    channels = []
    seen_urls = set()
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
    
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        for s in sources:
            try:
                resp = await client.get(s.url)
                if s.type == 'm3u':
                    # 增强匹配：提取 group-title
                    items = re.findall(r'#EXTINF:-1.*?(?:group-title="(.*?)")?.*?,(.*?)\n(http.*)', resp.text)
                    for group, name, url in items:
                        url = url.strip()
                        if url not in seen_urls:
                            channels.append({"name": name.strip(), "url": url, "group": group or "未分类"})
                            seen_urls.add(url)
                else:
                    for line in resp.text.split('\n'):
                        if ',' in line:
                            name, url = line.split(',', 1)
                            if url.strip() not in seen_urls:
                                channels.append({"name": name.strip(), "url": url.strip(), "group": "未分类"})
                                seen_urls.add(url.strip())
            except: continue

    output = f'#EXTM3U x-tvg-url="{base_url}/epg.xml"\n'
    for c in channels:
        logo = f"https://gcore.jsdelivr.net/gh/taksssss/tv/icon/{c['name']}.png"
        final_url = f"{base_url}/live/{c['name']}?url={urllib.parse.quote(c['url'], safe='')}" if proxy else c['url']
        output += f'#EXTINF:-1 tvg-name="{c["name"]}" tvg-logo="{logo}" group-title="{c["group"]}",{c["name"]}\n{final_url}\n'
    return Response(content=output, media_type="application/x-mpegurl")

# API: 获取状态
@app.get("/api/status")
async def get_status():
    return {
        "active_streams": [
            {"name": v["name"], "clients": len(v["queues"]), "speed": v["speed"], "info": v["info"]}
            for v in state.active_streams.values()
        ],
        "epg_status": "Loaded" if len(state.epg_data) > 100 else "Empty"
    }

# --- 基础管理 (保持原有 index/add/delete) ---
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
    return RedirectResponse(url="/", status_code=303)

@app.get("/delete/{source_id}")
async def delete_source(source_id: int):
    with Session(engine) as session:
        source = session.get(Source, source_id)
        if source: session.delete(source)
        session.commit()
    return RedirectResponse(url="/", status_code=303)

@app.get("/live/{channel_name}")
async def proxy_live(channel_name: str, url: str):
    return StreamingResponse(pool.subscribe(channel_name, url), media_type="video/mp2t")
