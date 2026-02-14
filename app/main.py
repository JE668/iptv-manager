import asyncio
import hashlib
import json
import os
import re
import time
import urllib.parse
import httpx
from fastapi import FastAPI, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Field, Session, SQLModel, create_engine, select

# --- 1. 数据库设置 ---
DB_FILE = "/app/data/iptv.db"
os.makedirs("/app/data", exist_ok=True)
engine = create_engine(f"sqlite:///{DB_FILE}")

class Source(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    url: str
    type: str

SQLModel.metadata.create_all(engine)
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# --- 2. 增强型自愈缓冲池 ---

class StreamPool:
    def __init__(self):
        self.streams = {}

    async def get_stream_info(self, url):
        """探测流信息，增加硬超时防止堵塞"""
        try:
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_streams', '-select_streams', 'v:0',
                '-analyzeduration', '3000000', '-probesize', '3000000', url
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            # 给 ffprobe 10秒时间，不行就撤
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10.0)
            data = json.loads(stdout)
            if 'streams' in data and len(data['streams']) > 0:
                s = data['streams'][0]
                return {
                    "res": f"{s.get('width')}x{s.get('height')}",
                    "codec": s.get('codec_name', '未知').upper(),
                    "fps": s.get('avg_frame_rate', '未知')
                }
        except:
            pass
        return {"res": "未知", "codec": "未知", "fps": "未知"}

    async def _fetcher(self, stream_id, url):
        """核心拉流器：具备超时监控和自动重连"""
        retry_count = 0
        while stream_id in self.streams:
            print(f"📡 尝试连接上游 (第{retry_count+1}次): {url}")
            start_time = time.time()
            bytes_count = 0
            
            try:
                # 如果是重连，探测可以跳过以加快速度
                if retry_count == 0:
                    info = await self.get_stream_info(url)
                    self.streams[stream_id]["info"] = info

                # 设置读取超时：如果 10 秒内上游没给任何数据，直接断开重连
                timeout = httpx.Timeout(10.0, read=10.0, connect=10.0)
                async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                    async with client.stream("GET", url) as r:
                        if r.status_code != 200:
                            raise Exception(f"HTTP Error {r.status_code}")
                        
                        retry_count = 0 # 连接成功，重置重试计数
                        async for chunk in r.aiter_bytes(chunk_size=256*1024): # 256KB 大块读取
                            bytes_count += len(chunk)
                            now = time.time()
                            if now - start_time >= 1.0:
                                speed = (bytes_count / 1024) / (now - start_time)
                                self.streams[stream_id]["speed"] = f"{speed:.1f} KB/s"
                                bytes_count = 0
                                start_time = now

                            # 如果没人看了，彻底退出
                            if not self.streams[stream_id]["queues"]:
                                return

                            # 分发给所有订阅者
                            for q in self.streams[stream_id]["queues"]:
                                try:
                                    # 如果某个下游满了，丢弃该块（防止慢客户端拖累所有人）
                                    q.put_nowait(chunk)
                                except asyncio.QueueFull:
                                    pass
            except Exception as e:
                print(f"❌ 流中断 ({e}), 3秒后尝试重连...")
                self.streams[stream_id]["speed"] = "重连中..."
                retry_count += 1
                await asyncio.sleep(3) # 等待3秒再重连，防止请求过频被封
            
            if retry_count > 10: # 连续失败10次，放弃
                print(f"💀 彻底失去连接: {url}")
                break
        
        self.streams.pop(stream_id, None)

    async def subscribe(self, name, url):
        stream_id = hashlib.md5(url.encode()).hexdigest()
        
        if stream_id not in self.streams:
            self.streams[stream_id] = {
                "name": name,
                "queues": [],
                "info": {"res": "探测中...", "codec": "探测中..."},
                "speed": "0 KB/s",
                "task": asyncio.create_task(self._fetcher(stream_id, url))
            }
        
        # 加大缓冲区：50个 256KB 块，约 12MB 缓存，应对严重抖动
        queue = asyncio.Queue(maxsize=50)
        self.streams[stream_id]["queues"].append(queue)
        
        try:
            while True:
                chunk = await queue.get()
                yield chunk
        finally:
            if stream_id in self.streams:
                self.streams[stream_id]["queues"].remove(queue)

stream_pool = StreamPool()

# --- 3. 聚合逻辑 ---

async def fetch_all_channels():
    unique_channels = []
    seen_urls = set()
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        for source in sources:
            try:
                resp = await client.get(source.url)
                if source.type == 'm3u':
                    pattern = re.compile(r'#EXTINF:-1.*?,(.*?)\n(http.*)')
                    for name, url in pattern.findall(resp.text):
                        u = url.strip()
                        if u not in seen_urls:
                            unique_channels.append({"name": name.strip(), "url": u})
                            seen_urls.add(u)
                else:
                    for line in resp.text.split('\n'):
                        if ',' in line:
                            parts = line.split(',', 1)
                            n, u = parts[0].strip(), parts[1].strip()
                            if u not in seen_urls:
                                unique_channels.append({"name": n, "url": u})
                                seen_urls.add(u)
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
    lines = [f"{c['name']},{f'{base_url}/live/{c['name']}?url={urllib.parse.quote(c['url'], safe='')}' if proxy else c['url']}" for c in channels]
    return Response(content="\n".join(lines), media_type="text/plain")

@app.get("/live/{channel_name}")
async def proxy_live(channel_name: str, url: str):
    return StreamingResponse(stream_pool.subscribe(channel_name, url), media_type="video/mp2t")

@app.get("/api/streams")
async def get_active_streams():
    return {data["name"]: {"clients": len(data["queues"]), "speed": data["speed"], "info": data["info"]} for d, data in stream_pool.streams.items()}

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
