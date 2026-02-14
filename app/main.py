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

# --- 1. 数据库与初始化 ---
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

# --- 2. 真正的一拉多缓冲池逻辑 ---

class StreamPool:
    def __init__(self):
        # 存储流信息和订阅者
        # { channel_name: {"queues": [], "task": Task, "info": {}, "speed": ""} }
        self.streams = {}

    async def get_stream_info(self, url):
        """增强版 ffprobe 探测"""
        try:
            # 增加探测时间限制和格式指定，提高成功率
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_streams', '-select_streams', 'v:0',
                '-analyzeduration', '2000000', '-probesize', '2000000', url
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8.0)
            data = json.loads(stdout)
            if 'streams' in data and len(data['streams']) > 0:
                s = data['streams'][0]
                return {
                    "res": f"{s.get('width')}x{s.get('height')}",
                    "codec": s.get('codec_name'),
                    "fps": s.get('avg_frame_rate')
                }
        except Exception as e:
            print(f"Probe Error: {e}")
        return {"res": "未知", "codec": "未知", "fps": "未知"}

    async def _fetcher(self, channel_name, url):
        """后台拉流单例任务"""
        print(f"🚀 启动上游拉流: {channel_name}")
        start_time = time.time()
        bytes_count = 0
        
        try:
            # 获取元数据
            info = await self.get_stream_info(url)
            self.streams[channel_name]["info"] = info

            async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
                async with client.stream("GET", url) as r:
                    async for chunk in r.aiter_bytes(chunk_size=128*1024): # 128KB 块
                        # 计算网速
                        bytes_count += len(chunk)
                        now = time.time()
                        if now - start_time >= 1.0:
                            speed = (bytes_count / 1024) / (now - start_time)
                            self.streams[channel_name]["speed"] = f"{speed:.1f} KB/s"
                            bytes_count = 0
                            start_time = now

                        # 分发给所有订阅者
                        if not self.streams[channel_name]["queues"]:
                            break # 没人看了，退出
                        
                        for q in self.streams[channel_name]["queues"]:
                            await q.put(chunk)
        except Exception as e:
            print(f"Fetcher Error: {e}")
        finally:
            print(f"🛑 停止上游拉流: {channel_name}")
            self.streams.pop(channel_name, None)

    async def subscribe(self, channel_name, url):
        """订阅流"""
        if channel_name not in self.streams:
            self.streams[channel_name] = {
                "queues": [],
                "task": asyncio.create_task(self._fetcher(channel_name, url)),
                "info": {"res": "探测中...", "codec": "探测中..."},
                "speed": "0 KB/s"
            }
        
        queue = asyncio.Queue(maxsize=50) # 缓冲区队列
        self.streams[channel_name]["queues"].append(queue)
        
        try:
            while True:
                chunk = await queue.get()
                yield chunk
        finally:
            # 离开时移除队列
            if channel_name in self.streams:
                self.streams[channel_name]["queues"].remove(queue)

stream_pool = StreamPool()

# --- 3. 路由设置 ---

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
    
    # 转换流状态供前端显示
    active_info = {
        name: {
            "clients": len(data["queues"]),
            "speed": data["speed"],
            "info": data["info"]
        } for name, data in stream_pool.streams.items()
    }
    
    return templates.TemplateResponse("index.html", {
        "request": request, "sources": sources, "active_streams": active_info
    })

@app.get("/api/streams")
async def get_active_streams():
    return {
        name: {
            "clients": len(data["queues"]),
            "speed": data["speed"],
            "info": data["info"]
        } for name, data in stream_pool.streams.items()
    }

@app.get("/live/{channel_name}")
async def proxy_live(channel_name: str, url: str):
    return StreamingResponse(stream_pool.subscribe(channel_name, url), media_type="video/mp2t")

@app.get("/playlist.m3u")
async def get_m3u(request: Request, proxy: bool = False):
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", str(request.base_url.netloc))
    base_url = f"{scheme}://{host}"
    
    channels = {}
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()

    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        for source in sources:
            try:
                resp = await client.get(source.url)
                if source.type == 'm3u':
                    lines = resp.text.split('\n')
                    for i in range(len(lines)):
                        if "#EXTINF" in lines[i]:
                            name = re.search(r',([^,]+)$', lines[i]).group(1).strip()
                            if i+1 < len(lines):
                                url = lines[i+1].strip()
                                if url.startswith("http") and name not in channels:
                                    channels[name] = url
                else:
                    for line in resp.text.split('\n'):
                        if ',' in line:
                            name, url = line.split(',', 1)
                            if name.strip() not in channels: channels[name.strip()] = url.strip()
            except: continue

    output = '#EXTM3U x-tvg-url="https://epg.170909.xyz:1799/t.xml.gz"\n'
    for name, url in channels.items():
        logo = f"https://gcore.jsdelivr.net/gh/taksssss/tv/icon/{name}.png"
        if proxy:
            encoded_url = urllib.parse.quote(url, safe='')
            final_url = f"{base_url}/live/{name}?url={encoded_url}"
        else:
            final_url = url
        output += f'#EXTINF:-1 tvg-name="{name}" tvg-logo="{logo}" group-title="聚合频道",{name}\n{final_url}\n'
    return Response(content=output, media_type="application/x-mpegurl")

# 保留之前的 add/delete 路由...
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
