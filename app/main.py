import asyncio
import hashlib
import json
import os
import re
import time
import urllib.parse
import gzip
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict

import httpx
from fastapi import FastAPI, Form, Request, Response, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Field, Session, SQLModel, create_engine, select
from sqlalchemy import text
from lxml import etree

# --- 1. 配置 ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IPTV-Manager")

ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin123")
SECRET_KEY = hashlib.sha256(ADMIN_PASS.encode()).hexdigest()

DB_FILE = "/app/data/iptv.db"
os.makedirs("/app/data", exist_ok=True)
engine = create_engine(f"sqlite:///{DB_FILE}", connect_args={"check_same_thread": False})

# 数据库模型
class Source(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    url: str
    type: str 
    status: str = "Unknown"
    last_check: float = 0

class EPGSource(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    url: str
    status: str = "Unknown"

class Setting(SQLModel, table=True):
    key: str = Field(primary_key=True)
    value: str

SQLModel.metadata.create_all(engine)

# 初始化设置
with Session(engine) as session:
    if not session.get(Setting, "epg_days"): session.add(Setting(key="epg_days", value="3"))
    if not session.get(Setting, "proxy_mode"): session.add(Setting(key="proxy_mode", value="1"))
    if not session.get(Setting, "epg_interval"): session.add(Setting(key="epg_interval", value="6")) # 默认6小时更新一次EPG
    session.commit()

app = FastAPI()
templates = Jinja2Templates(directory="templates")
LOGO_BASE = "https://gcore.jsdelivr.net/gh/taksssss/tv/icon/"

class GlobalState:
    def __init__(self):
        self.epg_xml = b""
        self.epg_gz = b""
        self.last_epg_update = 0
        self.is_epg_updating = False
state = GlobalState()

def is_authenticated(request: Request):
    return request.cookies.get("session_id") == SECRET_KEY

# --- 2. 增强型缓冲池 (BI 监控版) ---

class StreamPool:
    def __init__(self):
        self.streams: Dict = {}

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
                        start_time, bytes_in = time.time(), 0
                        async for chunk in r.aiter_bytes(chunk_size=128*1024):
                            if not self.streams[stream_id]["clients"]: return
                            bytes_in += len(chunk)
                            now = time.time()
                            if now - start_time >= 1.0:
                                self.streams[stream_id]["in_speed"] = f"{(bytes_in / 1024 / (now-start_time)):.1f} KB/s"
                                bytes_in, start_time = 0, now
                            for q in self.streams[stream_id]["queues"]:
                                try: q.put_nowait(chunk)
                                except: pass
            except:
                retry_count += 1
                if stream_id in self.streams: self.streams[stream_id]["in_speed"] = f"重连({retry_count})"
                if retry_count > 10: break
                await asyncio.sleep(2)
        self.streams.pop(stream_id, None)

    async def subscribe(self, name: str, url: str, client_ip: str):
        stream_id = hashlib.md5(url.encode()).hexdigest()
        client_id = hashlib.md5(f"{client_ip}{time.time()}".encode()).hexdigest()[:8]
        if stream_id not in self.streams:
            self.streams[stream_id] = {"name": name, "url": url, "queues": [], "clients": {}, "info": {"res": "探测中...", "codec": "探测中..."}, "in_speed": "0 KB/s", "out_speed": "0 KB/s"}
            asyncio.create_task(self._fetcher(stream_id, url, name))
            await asyncio.sleep(0.5)
        queue = asyncio.Queue(maxsize=100)
        self.streams[stream_id]["queues"].append(queue)
        self.streams[stream_id]["clients"][client_id] = {"ip": client_ip, "out_bytes": 0, "speed": "0 KB/s", "last_ts": time.time()}
        try:
            while True:
                chunk = await queue.get()
                c = self.streams[stream_id]["clients"][client_id]
                c["out_bytes"] += len(chunk)
                now = time.time()
                if now - c["last_ts"] >= 1.0:
                    c["speed"] = f"{(c['out_bytes'] / 1024 / (now-c['last_ts'])):.1f} KB/s"
                    c["out_bytes"], c["last_ts"] = 0, now
                    # 更新该流的总下游速度
                    total_out = sum([float(cli["speed"].split(' ')[0]) for cli in self.streams[stream_id]["clients"].values()])
                    self.streams[stream_id]["out_speed"] = f"{total_out:.1f} KB/s"
                self.streams[stream_id]["buffer_level"] = queue.qsize()
                yield chunk
        finally:
            if stream_id in self.streams:
                self.streams[stream_id]["queues"].remove(queue)
                self.streams[stream_id]["clients"].pop(client_id, None)

stream_pool = StreamPool()

# --- 3. EPG 聚合逻辑 (独立定时更新) ---

async def update_epg_task():
    """独立的后台 EPG 维护任务"""
    if state.is_epg_updating: return
    state.is_epg_updating = True
    logger.info("⏰ 启动定时 EPG 聚合...")
    
    master_root = etree.Element("tv")
    with Session(engine) as session:
        sources = session.exec(select(EPGSource)).all()
        days_limit = int(session.get(Setting, "epg_days").value)
    
    cutoff = datetime.now() - timedelta(days=1)
    end = datetime.now() + timedelta(days=days_limit)
    
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        for s in sources:
            try:
                r = await client.get(s.url)
                content = r.content
                if s.url.endswith(".gz") or content[:2] == b'\x1f\x8b': content = gzip.decompress(content)
                root = etree.fromstring(content, parser=etree.XMLParser(recover=True))
                for channel in root.xpath("//channel"): master_root.append(channel)
                for prog in root.xpath("//programme"):
                    try:
                        st = datetime.strptime(prog.get("start")[:14], "%Y%m%d%H%M%S")
                        if cutoff <= st <= end: master_root.append(prog)
                    except: pass
                s.status = "Success"
            except: s.status = "Error"
            with Session(engine) as session: session.add(s); session.commit()
    
    final_xml = etree.tostring(master_root, encoding="UTF-8", xml_declaration=True, pretty_print=True)
    state.epg_xml = final_xml
    state.epg_gz = gzip.compress(final_xml)
    state.last_epg_update = time.time()
    state.is_epg_updating = False

async def epg_maintenance_loop():
    while True:
        with Session(engine) as session:
            interval = int(session.get(Setting, "epg_interval").value)
        await update_epg_task()
        await asyncio.sleep(interval * 3600)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(epg_maintenance_loop())

# --- 4. 实时订阅更新 (即时触发) ---

async def fetch_realtime_sources():
    """每次被调用时实时抓取订阅源内容"""
    unique_channels, seen_urls = [], set()
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
        proxy_mode = int(session.get(Setting, "proxy_mode").value)
    
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        # 使用 gather 并行抓取提升速度
        tasks = [client.get(s.url) for s in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, resp in enumerate(results):
            if isinstance(resp, Exception): continue
            s = sources[i]
            lines = resp.text.split('\n')
            
            if s.type == 'm3u':
                for j in range(len(lines)):
                    line = lines[j].strip()
                    if line.startswith("#EXTINF"):
                        raw_name = line.split(',')[-1].strip()
                        for k in range(j+1, min(j+5, len(lines))):
                            u = lines[k].strip()
                            if u.startswith("http") and u not in seen_urls:
                                use_proxy = (proxy_mode == 2) or (proxy_mode == 1 and ("/udp/" in u or "/rtp/" in u))
                                unique_channels.append({"name": raw_name, "url": u, "use_proxy": use_proxy})
                                seen_urls.add(u); break
            else:
                for line in lines:
                    if ',' in line:
                        name, url = line.split(',', 1); u = url.strip()
                        if u not in seen_urls:
                            use_proxy = (proxy_mode == 2) or (proxy_mode == 1 and ("/udp/" in u or "/rtp/" in u))
                            unique_channels.append({"name": name.strip(), "url": u, "use_proxy": use_proxy})
                            seen_urls.add(u)
    return unique_channels

# --- 5. 路由 ---

@app.get("/playlist.m3u")
@app.get("/playlist.txt")
async def get_playlist(request: Request):
    """即时触发更新订阅"""
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", str(request.base_url.netloc))
    base_url = f"{scheme}://{host}"
    
    channels = await fetch_realtime_sources()
    
    if request.url.path.endswith('.txt'):
        lines = []
        for c in channels:
            url = f"{base_url}/live/{c['name']}?url={urllib.parse.quote(c['url'], safe='')}" if c['use_proxy'] else c['url']
            lines.append(f"{c['name']},{url}")
        return Response(content="\n".join(lines), media_type="text/plain")
    
    output = f'#EXTM3U x-tvg-url="{base_url}/epg.xml.gz"\n'
    for c in channels:
        url = f"{base_url}/live/{c['name']}?url={urllib.parse.quote(c['url'], safe='')}" if c['use_proxy'] else c['url']
        output += f'#EXTINF:-1 tvg-logo="{LOGO_BASE}{c["name"].upper()}.png",{c["name"]}\n{url}\n'
    return Response(content=output, media_type="application/x-mpegurl")

@app.get("/api/status")
async def get_api_status(request: Request):
    if not is_authenticated(request): return {"active_streams": []}
    active = []
    total_in, total_out, total_peers = 0, 0, 0
    for s_id, data in stream_pool.streams.items():
        try:
            total_in += float(data["in_speed"].split(' ')[0])
            total_out += float(data["out_speed"].split(' ')[0])
        except: pass
        total_peers += len(data["clients"])
        active.append({
            "name": data["name"], "url": data["url"], "in_speed": data["in_speed"], 
            "out_speed": data["out_speed"], "peers": len(data["clients"]),
            "info": data["info"], "buffer": f"{data.get('buffer_level', 0)}/100",
            "clients": list(data["clients"].values())
        })
    return {
        "active_streams": active, "is_checking": state.is_epg_updating,
        "last_epg": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(state.last_epg_update)) if state.last_epg_update else "从未更新",
        "kpis": {
            "total_in": f"{total_in:.1f} KB/s", "total_out": f"{total_out:.1f} KB/s",
            "stream_count": len(active), "peer_count": total_peers
        }
    }

# --- 后台管理路由 (基础功能) ---
@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request): return templates.TemplateResponse("login.html", {"request": request})
@app.post("/login")
async def login(username: str = Form(...), password: str = Form(...)):
    if username == ADMIN_USER and password == ADMIN_PASS:
        resp = RedirectResponse(url="/", status_code=303)
        resp.set_cookie(key="session_id", value=SECRET_KEY, max_age=604800, httponly=True); return resp
    return RedirectResponse(url="/login?error=1", status_code=303)
@app.get("/logout")
async def logout():
    resp = RedirectResponse(url="/login", status_code=303); resp.delete_cookie("session_id"); return resp
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        return templates.TemplateResponse("index.html", {
            "request": request, "sources": session.exec(select(Source)).all(), 
            "epg_sources": session.exec(select(EPGSource)).all(),
            "epg_days": session.get(Setting, "epg_days").value,
            "proxy_mode": session.get(Setting, "proxy_mode").value
        })
@app.get("/live/{channel_name}")
async def proxy_live(request: Request, channel_name: str, url: str):
    client_ip = request.headers.get("x-real-ip") or request.client.host
    return StreamingResponse(stream_pool.subscribe(channel_name, url, client_ip), media_type="video/mp2t")
@app.get("/epg.xml")
async def get_epg(): return Response(content=state.epg_xml, media_type="application/xml")
@app.get("/epg.xml.gz")
async def get_epg_gz(): return Response(content=state.epg_gz, media_type="application/gzip")
@app.post("/add_source")
async def add_source(request: Request, name: str = Form(...), url: str = Form(...), type: str = Form(...)):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session: session.add(Source(name=name, url=url, type=type)); session.commit()
    return RedirectResponse(url="/", status_code=303)
@app.get("/delete/{sid}")
async def del_s(sid: int):
    with Session(engine) as session:
        s = session.get(Source, sid)
        if s: session.delete(s); session.commit()
    return RedirectResponse(url="/", status_code=303)
@app.post("/add_epg_source")
async def add_epg(name: str = Form(...), url: str = Form(...)):
    with Session(engine) as session: session.add(EPGSource(name=name, url=url)); session.commit()
    asyncio.create_task(update_epg_task()); return RedirectResponse(url="/", status_code=303)
@app.get("/delete_epg/{eid}")
async def del_e(eid: int):
    with Session(engine) as session:
        e = session.get(EPGSource, eid)
        if e: session.delete(e); session.commit()
    return RedirectResponse(url="/", status_code=303)
@app.post("/update_global_settings")
async def update_settings(request: Request, proxy_mode: str = Form(...), epg_days: str = Form(...)):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        session.get(Setting, "proxy_mode").value = proxy_mode
        session.get(Setting, "epg_days").value = epg_days
        session.commit()
    return RedirectResponse(url="/", status_code=303)
@app.get("/refresh")
async def ref(): asyncio.create_task(update_epg_task()); return RedirectResponse(url="/", status_code=303)
