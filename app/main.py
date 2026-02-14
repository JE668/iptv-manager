from fastapi import FastAPI, Request, Form, Response, BackgroundTasks
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

# 存储当前的流状态
active_streams = {}

async def get_stream_info(url):
    """利用 ffprobe 获取视频流元数据"""
    try:
        cmd = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json', 
            '-show_streams', '-select_streams', 'v:0', url
        ]
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
        # 启动后台检测
        asyncio.create_task(update_stream_metadata(channel_name, url))

    active_streams[channel_name]["clients"] += 1
    
    try:
        async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
            async with client.stream("GET", url) as r:
                start_time = time.time()
                chunk_counter = 0
                async for chunk in r.aiter_bytes(chunk_size=1024*64):
                    chunk_counter += len(chunk)
                    elapsed = time.time() - start_time
                    if elapsed >= 1.0:
                        speed_kb = (chunk_counter / 1024) / elapsed
                        active_streams[channel_name]["speed"] = f"{speed_kb:.1f} KB/s"
                        chunk_counter = 0
                        start_time = time.time()
                    yield chunk
    finally:
        if channel_name in active_streams:
            active_streams[channel_name]["clients"] -= 1
            if active_streams[channel_name]["clients"] <= 0:
                active_streams.pop(channel_name, None)

async def update_stream_metadata(channel_name, url):
    info = await get_stream_info(url)
    if channel_name in active_streams:
        active_streams[channel_name]["info"] = info

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
    return active_streams

@app.get("/live/{channel_name}")
async def proxy_live(channel_name: str, url: str):
    """代理播放接口"""
    # 打印一下，方便在后台日志看 URL 是否正确
    print(f"Proxying channel: {channel_name} -> {url}")
    return StreamingResponse(stream_generator(channel_name, url), media_type="video/mp2t")

@app.get("/playlist.m3u")
async def get_m3u(request: Request, proxy: bool = False):
    channels = {}
    
    # --- 自动识别 HTTPS 协议的核心逻辑 ---
    # 检查反向代理传来的协议头部，如果没有则使用本地请求协议
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    # 获取外部访问的主机名（域名）
    host = request.headers.get("host", str(request.base_url.netloc))
    # 拼接出正确的 Base URL
    base_url = f"{scheme}://{host}"
    
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
                            name_match = re.search(r',([^,]+)$', lines[i])
                            if name_match:
                                name = name_match.group(1).strip()
                                if i+1 < len(lines):
                                    url = lines[i+1].strip()
                                    if url.startswith("http") and name not in channels:
                                        channels[name] = url
                else:
                    for line in resp.text.split('\n'):
                        if ',' in line:
                            name, url = line.split(',', 1)
                            if name.strip() not in channels: 
                                channels[name.strip()] = url.strip()
            except: continue

    output = f'#EXTM3U x-tvg-url="https://epg.170909.xyz:1799/t.xml.gz"\n'
    for name, url in channels.items():
        logo = f"https://gcore.jsdelivr.net/gh/taksssss/tv/icon/{name}.png"
        
        # 关键点：如果开启代理模式，使用生成的 https 基础链接
        # 这样客户端通过 HTTPS 请求视频流，Docker 内部再去 HTTP 请求上游，从而解决不兼容问题
        if proxy:
            # 注意：url 需要进行简单的二次编码，防止参数冲突
            import urllib.parse
            encoded_url = urllib.parse.quote(url, safe='')
            final_url = f"{base_url}/live/{name}?url={encoded_url}"
        else:
            final_url = url
            
        output += f'#EXTINF:-1 tvg-name="{name}" tvg-logo="{logo}" group-title="聚合频道",{name}\n{final_url}\n'
    
    return Response(content=output, media_type="application/x-mpegurl")
    
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
