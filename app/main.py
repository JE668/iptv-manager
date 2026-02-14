from fastapi import FastAPI, Request, Form, Response, Background_Tasks
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import SQLModel, Field, Session, select, create_engine
import httpx
import re
import os
import asyncio
import time
import subprocess
import json

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

# --- 2. 流媒体监控与缓冲池逻辑 ---

# 存储当前的流状态：{ channel_name: { clients: 0, speed: 0, info: {}, history_bytes: [] } }
active_streams = {}

async def get_stream_info(url):
    """利用 ffprobe 获取视频流元数据"""
    try:
        cmd = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json', 
            '-show_streams', '-select_streams', 'v:0', url
        ]
        # 设置 5 秒超时，防止卡死
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5.0)
        data = json.loads(stdout)
        if 'streams' in data and len(data['streams']) > 0:
            s = data['streams'][0]
            return {
                "res": f"{s.get('width')}x{s.get('height')}",
                "codec": s.get('codec_name'),
                "fps": s.get('avg_frame_rate')
            }
    except:
        pass
    return {"res": "未知", "codec": "未知", "fps": "未知"}

async def stream_generator(channel_name: str, url: str):
    """流代理核心生成器"""
    if channel_name not in active_streams:
        active_streams[channel_name] = {
            "url": url, "clients": 0, "speed": "0 KB/s", 
            "info": {"res": "检测中...", "codec": "检测中..."},
            "start_time": time.time(), "bytes_count": 0
        }
        # 异步启动 ffprobe 检测信息
        async def update_info():
            info = await get_stream_info(url)
            if channel_name in active_streams:
                active_streams[channel_name]["info"] = info
        asyncio.create_task(update_info())

    active_streams[channel_name]["clients"] += 1
    
    try:
        async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
            async with client.stream("GET", url) as r:
                start_time = time.time()
                chunk_counter = 0
                async for chunk in r.aiter_bytes(chunk_size=1024*64): # 64KB 缓冲
                    chunk_counter += len(chunk)
                    # 每秒计算一次网速
                    elapsed = time.time() - start_time
                    if elapsed >= 1.0:
                        speed_kb = (chunk_counter / 1024) / elapsed
                        active_streams[channel_name]["speed"] = f"{speed_kb:.1f} KB/s"
                        chunk_counter = 0
                        start_time = time.time()
                    
                    yield chunk
    finally:
        active_streams[channel_name]["clients"] -= 1
        if active_streams[channel_name]["clients"] <= 0:
            active_streams.pop(channel_name, None)

# --- 3. 路由设置 ---

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "sources": sources, 
        "active_streams": active_streams
    })

@app.get("/api/streams")
async def get_active_streams():
    """给前端 Dashboard 调用的接口，实现不刷新页面更新数据"""
    return active_streams

@app.get("/live/{channel_name}")
async def proxy_live(channel_name: str, url: str):
    """代理播放接口"""
    return StreamingResponse(stream_generator(channel_name, url), media_type="video/mp2t")

@app.get("/playlist.m3u")
async def get_m3u(request: Request, proxy: bool = False):
    """
    生成 M3U。
    如果不带参数，输出原链接。
    如果访问 /playlist.m3u?proxy=True，则输出代理链接。
    """
    channels = {}
    base_url = str(request.base_url).rstrip('/')
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
                            url = lines[i+1].strip()
                            if name not in channels: channels[name] = url
                else:
                    for line in resp.text.split('\n'):
                        if ',' in line:
                            name, url = line.split(',', 1)
                            if name.strip() not in channels: channels[name.strip()] = url.strip()
            except: continue

    output = '#EXTM3U x-tvg-url="https://epg.170909.xyz:1799/t.xml.gz"\n'
    for name, url in channels.items():
        logo = f"https://gcore.jsdelivr.net/gh/taksssss/tv/icon/{name}.png"
        # 如果开启代理模式，将 URL 指向本地 /live 接口
        final_url = f"{base_url}/live/{name}?url={url}" if proxy else url
        output += f'#EXTINF:-1 tvg-name="{name}" tvg-logo="{logo}" group-title="聚合频道",{name}\n{final_url}\n'
    
    return Response(content=output, media_type="application/x-mpegurl")

# 保留增删逻辑
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
