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

# --- 1. 数据库与初始化 ---
DB_FILE = "/app/data/iptv.db"
os.makedirs("/app/data", exist_ok=True)
engine = create_engine(f"sqlite:///{DB_FILE}")

class Source(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    url: str
    type: str # m3u 或 txt

SQLModel.metadata.create_all(engine)
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# --- 2. 增强型缓冲池与探测引擎 ---

class StreamPool:
    def __init__(self):
        # Key 改为 URL 的哈希值，防止同名频道冲突
        self.streams = {}

    async def get_stream_info(self, url):
        """深度探测：增加分析时长，提高编码和分辨率准确度"""
        try:
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_streams', '-select_streams', 'v:0',
                # 提高探测上限以识别复杂编码
                '-analyzeduration', '5000000', 
                '-probesize', '5000000', 
                url
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10.0)
            data = json.loads(stdout)
            if 'streams' in data and len(data['streams']) > 0:
                s = data['streams'][0]
                res = f"{s.get('width')}x{s.get('height')}"
                codec = s.get('codec_name', '未知').upper()
                fps = s.get('avg_frame_rate', '未知')
                return {"res": res, "codec": codec, "fps": fps}
        except:
            pass
        return {"res": "未知", "codec": "未知", "fps": "未知"}

    async def _fetcher(self, stream_id, url):
        print(f"🚀 启动拉流任务: {stream_id}")
        start_time = time.time()
        bytes_count = 0
        try:
            # 探测信息
            info = await self.get_stream_info(url)
            if stream_id in self.streams:
                self.streams[stream_id]["info"] = info

            async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
                async with client.stream("GET", url) as r:
                    async for chunk in r.aiter_bytes(chunk_size=128*1024):
                        bytes_count += len(chunk)
                        now = time.time()
                        if now - start_time >= 1.0:
                            speed = (bytes_count / 1024) / (now - start_time)
                            if stream_id in self.streams:
                                self.streams[stream_id]["speed"] = f"{speed:.1f} KB/s"
                            bytes_count = 0
                            start_time = now

                        if stream_id not in self.streams or not self.streams[stream_id]["queues"]:
                            break
                        for q in self.streams[stream_id]["queues"]:
                            await q.put(chunk)
        finally:
            self.streams.pop(stream_id, None)

    async def subscribe(self, name, url):
        # 使用 URL 的哈希作为唯一标识，避免同名冲突
        stream_id = hashlib.md5(url.encode()).hexdigest()
        
        if stream_id not in self.streams:
            self.streams[stream_id] = {
                "name": name,
                "queues": [],
                "info": {"res": "探测中...", "codec": "探测中..."},
                "speed": "0 KB/s",
                "task": asyncio.create_task(self._fetcher(stream_id, url))
            }
        
        queue = asyncio.Queue(maxsize=100)
        self.streams[stream_id]["queues"].append(queue)
        try:
            while True:
                chunk = await queue.get()
                yield chunk
        finally:
            if stream_id in self.streams:
                self.streams[stream_id]["queues"].remove(queue)

stream_pool = StreamPool()

# --- 3. 聚合逻辑：基于 URL 去重 ---

async def fetch_all_channels():
    """统一获取所有源并去重（基于 URL）"""
    unique_channels = []
    seen_urls = set()
    
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()

    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        for source in sources:
            try:
                resp = await client.get(source.url)
                if source.type == 'm3u':
                    content = resp.text
                    # 匹配 #EXTINF 和 URL
                    pattern = re.compile(r'#EXTINF:-1.*?,(.*?)\n(http.*)')
                    for name, url in pattern.findall(content):
                        url = url.strip()
                        if url not in seen_urls:
                            unique_channels.append({"name": name.strip(), "url": url})
                            seen_urls.add(url)
                else:
                    for line in resp.text.split('\n'):
                        if ',' in line:
                            parts = line.split(',', 1)
                            name, url = parts[0].strip(), parts[1].strip()
                            if url not in seen_urls:
                                unique_channels.append({"name": name, "url": url})
                                seen_urls.add(url)
            except: continue
    return unique_channels

# --- 4. 路由设置 ---

@app.get("/playlist.m3u")
async def get_m3u(request: Request, proxy: bool = False):
    channels = await fetch_all_channels()
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", str(request.base_url.netloc))
    base_url = f"{scheme}://{host}"
    
    output = '#EXTM3U x-tvg-url="https://epg.170909.xyz:1799/t.xml.gz"\n'
    for c in channels:
        name, url = c["name"], c["url"]
        logo = f"https://gcore.jsdelivr.net/gh/taksssss/tv/icon/{name}.png"
        if proxy:
            encoded_url = urllib.parse.quote(url, safe='')
            final_url = f"{base_url}/live/{name}?url={encoded_url}"
        else:
            final_url = url
        output += f'#EXTINF:-1 tvg-name="{name}" tvg-logo="{logo}" group-title="聚合频道",{name}\n{final_url}\n'
    return Response(content=output, media_type="application/x-mpegurl")

@app.get("/playlist.txt")
async def get_txt(request: Request, proxy: bool = False):
    channels = await fetch_all_channels()
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", str(request.base_url.netloc))
    base_url = f"{scheme}://{host}"
    
    lines = []
    for c in channels:
        name, url = c["name"], c["url"]
        if proxy:
            encoded_url = urllib.parse.quote(url, safe='')
            final_url = f"{base_url}/live/{name}?url={encoded_url}"
        else:
            final_url = url
        lines.append(f"{name},{final_url}")
    return Response(content="\n".join(lines), media_type="text/plain")

@app.get("/live/{channel_name}")
async def proxy_live(channel_name: str, url: str):
    return StreamingResponse(stream_pool.subscribe(channel_name, url), media_type="video/mp2t")

@app.get("/api/streams")
async def get_active_streams():
    return {
        # 返回流名称而不是哈希值
        data["name"]: {
            "clients": len(data["queues"]),
            "speed": data["speed"],
            "info": data["info"]
        } for stream_id, data in stream_pool.streams.items()
    }

# --- 基础管理路由 (保持不变) ---
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
        if source:
            session.delete(source)
            session.commit()
    return RedirectResponse(url="/", status_code=303)
