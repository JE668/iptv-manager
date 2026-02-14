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
from typing import Optional, List, Dict

import httpx
from fastapi import FastAPI, Form, Request, Response, BackgroundTasks, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Field, Session, SQLModel, create_engine, select
from sqlalchemy import text

# --- 1. 配置与安全设置 ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IPTV-Manager")

# 从环境变量读取账号密码，默认 admin / admin123
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin123")
# 用于 Cookie 加密的密钥
SECRET_KEY = hashlib.sha256(ADMIN_PASS.encode()).hexdigest()

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

app = FastAPI()
templates = Jinja2Templates(directory="templates")
LOGO_BASE = "https://gcore.jsdelivr.net/gh/taksssss/tv/icon/"

class GlobalState:
    def __init__(self):
        self.epg_content = b""
        self.is_checking = False

state = GlobalState()

# --- 2. 身份验证助手 ---

def is_authenticated(request: Request):
    """检查用户是否已登录"""
    return request.cookies.get("session_id") == SECRET_KEY

def login_required(request: Request):
    """用于保护路由的快捷判断"""
    if not is_authenticated(request):
        raise HTTPException(status_code=303, detail="Not Authenticated")

# --- 3. 登录/注销路由 ---

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if is_authenticated(request):
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login(username: str = Form(...), password: str = Form(...)):
    if username == ADMIN_USER and password == ADMIN_PASS:
        response = RedirectResponse(url="/", status_code=303)
        # 设置持久化 Cookie，有效期 7 天
        response.set_cookie(key="session_id", value=SECRET_KEY, max_age=604800, httponly=True)
        return response
    return RedirectResponse(url="/login?error=1", status_code=303)

@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie("session_id")
    return response

# --- 4. 管理路由 (增加安全检查) ---

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if not is_authenticated(request):
        return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
    return templates.TemplateResponse("index.html", {"request": request, "sources": sources})

@app.get("/api/status")
async def get_api_status(request: Request):
    if not is_authenticated(request):
        return {"active_streams": [], "is_checking": False, "auth": False}
    
    active = []
    for s_id, data in stream_pool.streams.items():
        active.append({
            "name": data["name"],
            "url": data["url"],
            "clients": list(data["clients"].values()),
            "client_count": len(data["clients"]),
            "speed": data["speed"],
            "info": data["info"]
        })
    return {"active_streams": active, "is_checking": state.is_checking, "auth": True}

# --- 此处复用之前的 StreamPool, fetch_all, maintenance 等所有逻辑 ---
# (为了节省篇幅，以下仅展示关键变动，请确保将其余逻辑完整保留)

class StreamPool:
    def __init__(self): self.streams: Dict = {}
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
        while stream_id in self.streams:
            if not self.streams[stream_id]["clients"]: break
            try:
                if retry_count == 0: self.streams[stream_id]["info"] = await self.get_stream_info(url)
                async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                    async with client.stream("GET", url) as r:
                        if r.status_code != 200: raise Exception()
                        retry_count = 0
                        start_time, bytes_count = time.time(), 0
                        async for chunk in r.aiter_bytes(chunk_size=128*1024):
                            if not self.streams[stream_id]["clients"]: return
                            bytes_count += len(chunk)
                            now = time.time()
                            if now - start_time >= 1.0:
                                speed = (bytes_count / 1024) / (now - start_time)
                                self.streams[stream_id]["speed"] = f"{speed:.1f} KB/s"
                                bytes_count, start_time = 0, now
                            for q in self.streams[stream_id]["queues"]:
                                try: q.put_nowait(chunk)
                                except: pass
            except:
                retry_count += 1
                if stream_id in self.streams: self.streams[stream_id]["speed"] = f"重连中({retry_count})"
                if retry_count > 10: break
                await asyncio.sleep(2)
        self.streams.pop(stream_id, None)

    async def subscribe(self, name: str, url: str, client_ip: str):
        stream_id = hashlib.md5(url.encode()).hexdigest()
        client_id = hashlib.md5(f"{client_ip}{time.time()}".encode()).hexdigest()[:8]
        if stream_id not in self.streams:
            self.streams[stream_id] = {"name": name, "url": url, "queues": [], "clients": {}, "info": {"res": "探测中...", "codec": "探测中..."}, "speed": "0 KB/s"}
            asyncio.create_task(self._fetcher(stream_id, url, name))
            await asyncio.sleep(0.5)
        queue = asyncio.Queue(maxsize=100)
        self.streams[stream_id]["queues"].append(queue)
        self.streams[stream_id]["clients"][client_id] = {"ip": client_ip, "start_time": datetime.now().strftime("%H:%M:%S")}
        try:
            while True:
                chunk = await queue.get()
                yield chunk
        finally:
            if stream_id in self.streams:
                self.streams[stream_id]["queues"].remove(queue)
                self.streams[stream_id]["clients"].pop(client_id, None)

stream_pool = StreamPool()

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
            if r.status_code == 200: state.epg_content = gzip.decompress(r.content)
        except: pass
    state.is_checking = False

async def maintenance_loop():
    while True: await run_maintenance(); await asyncio.sleep(4 * 3600)
@app.on_event("startup")
async def startup_event(): asyncio.create_task(maintenance_loop())

# --- 公开接口 (播放器使用) ---

@app.get("/playlist.m3u")
@app.get("/playlist.txt")
async def get_playlist(request: Request, proxy: bool = False):
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", str(request.base_url.netloc))
    base_url = f"{scheme}://{host}"
    unique_channels, seen_urls = [], set()
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for s in sources:
            try:
                resp = await client.get(s.url); lines = resp.text.split('\n')
                if s.type == 'm3u':
                    for i in range(len(lines)):
                        line = lines[i].strip()
                        if line.startswith("#EXTINF"):
                            group_match = re.search(r'group-title="(.*?)"', line)
                            raw_name = line.split(',')[-1].strip()
                            clean_name = raw_name.upper().replace(" ", "") # 简化版清洗
                            for j in range(i + 1, min(i + 5, len(lines))):
                                u = lines[j].strip()
                                if u.startswith("http") and u not in seen_urls:
                                    unique_channels.append({"name": raw_name, "url": u, "group": group_match.group(1) if group_match else "聚合"})
                                    seen_urls.add(u); break
                else:
                    for line in lines:
                        if ',' in line:
                            name, url = line.split(',', 1); u = url.strip()
                            if u not in seen_urls:
                                unique_channels.append({"name": name.strip(), "url": u, "group": "聚合"}); seen_urls.add(u)
            except: continue
    
    unique_channels.sort(key=lambda x: (x['group'], x['name']))
    if request.url.path.endswith('.txt'):
        lines, current_group = [], ""
        for c in unique_channels:
            if c['group'] != current_group: current_group = c['group']; lines.append(f"{current_group},#genre#")
            url = f"{base_url}/live/{c['name']}?url={urllib.parse.quote(c['url'], safe='')}" if proxy else c['url']
            lines.append(f"{c['name']},{url}")
        return Response(content="\n".join(lines), media_type="text/plain")
    else:
        output = f'#EXTM3U x-tvg-url="{base_url}/epg.xml"\n'
        for c in unique_channels:
            logo = f"{LOGO_BASE}{c['name']}.png"
            url = f"{base_url}/live/{c['name']}?url={urllib.parse.quote(c['url'], safe='')}" if proxy else c['url']
            output += f'#EXTINF:-1 tvg-name="{c["name"]}" tvg-logo="{logo}" group-title="{c["group"]}",{c["name"]}\n{url}\n'
        return Response(content=output, media_type="application/x-mpegurl")

@app.get("/live/{channel_name}")
async def proxy_live(request: Request, channel_name: str, url: str):
    client_ip = request.headers.get("x-real-ip") or request.client.host
    return StreamingResponse(stream_pool.subscribe(channel_name, url, client_ip), media_type="video/mp2t")

@app.get("/epg.xml")
async def get_epg():
    if not state.epg_content: return Response(content='<?xml version="1.0" encoding="UTF-8"?><tv></tv>', media_type="application/xml")
    return Response(content=state.epg_content, media_type="application/xml")

# --- 操作路由 (需要登录) ---

@app.post("/add_source")
async def add_source(request: Request, name: str = Form(...), url: str = Form(...), type: str = Form(...)):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session: session.add(Source(name=name, url=url, type=type)); session.commit()
    asyncio.create_task(run_maintenance()); return RedirectResponse(url="/", status_code=303)

@app.get("/delete/{source_id}")
async def delete_source(request: Request, source_id: int):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        source = session.get(Source, source_id)
        if source: session.delete(source); session.commit()
    return RedirectResponse(url="/", status_code=303)

@app.get("/refresh")
async def manual_refresh(request: Request):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    asyncio.create_task(run_maintenance()); return RedirectResponse(url="/", status_code=303)
