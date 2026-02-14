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

# --- 1. 配置与数据库 ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IPTV-Manager")

ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin123")
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

class EPGSource(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    url: str
    status: str = "Unknown"

# 新增：存储全局配置（如 EPG 天数）
class Setting(SQLModel, table=True):
    key: str = Field(primary_key=True)
    value: str

SQLModel.metadata.create_all(engine)

# 自动迁移
with engine.connect() as conn:
    try:
        inspector = conn.execute(text("PRAGMA table_info(source)"))
        columns = [row[1] for row in inspector]
        if "status" not in columns: conn.execute(text("ALTER TABLE source ADD COLUMN status VARCHAR DEFAULT 'Unknown'"))
        # 初始化设置
        with Session(engine) as session:
            if not session.get(Setting, "epg_days"):
                session.add(Setting(key="epg_days", value="3"))
                session.commit()
    except: pass

app = FastAPI()
templates = Jinja2Templates(directory="templates")
LOGO_BASE = "https://gcore.jsdelivr.net/gh/taksssss/tv/icon/"

class GlobalState:
    def __init__(self):
        self.epg_xml = b""
        self.epg_gz = b""
        self.is_checking = False

state = GlobalState()

# --- 2. 身份验证 ---
def is_authenticated(request: Request):
    return request.cookies.get("session_id") == SECRET_KEY

# --- 3. EPG 聚合引擎 ---

def parse_epg_time(t_str):
    """解析 XMLTV 时间格式 20231024120000 +0800"""
    try:
        return datetime.strptime(t_str[:14], "%Y%m%d%H%M%S")
    except:
        return None

async def merge_epg_data():
    """下载、过滤并聚合多个 EPG 源"""
    logger.info("开始聚合 EPG 数据...")
    
    with Session(engine) as session:
        epg_sources = session.exec(select(EPGSource)).all()
        days_limit = int(session.get(Setting, "epg_days").value)

    if not epg_sources:
        state.epg_xml = b'<?xml version="1.0" encoding="UTF-8"?><tv></tv>'
        return

    master_root = etree.Element("tv")
    master_root.set("generator-info-name", "NextGen-IPTV-Manager")
    
    seen_channel_ids = set()
    cutoff_date = datetime.now() - timedelta(days=1) # 昨天之后的
    end_date = datetime.now() + timedelta(days=days_limit) # 限制未来天数

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        for source in epg_sources:
            try:
                r = await client.get(source.url)
                content = r.content
                # 如果是 gz，解压
                if source.url.endswith(".gz") or content[:2] == b'\x1f\x8b':
                    content = gzip.decompress(content)
                
                parser = etree.XMLParser(recover=True)
                root = etree.fromstring(content, parser=parser)
                
                # 1. 提取频道定义
                for channel in root.xpath("//channel"):
                    c_id = channel.get("id")
                    if c_id not in seen_channel_ids:
                        master_root.append(channel)
                        seen_channel_ids.add(c_id)
                
                # 2. 提取节目单并过滤时间
                for prog in root.xpath("//programme"):
                    start_time = parse_epg_time(prog.get("start"))
                    if start_time and cutoff_date <= start_time <= end_date:
                        master_root.append(prog)
                
                source.status = "Success"
            except Exception as e:
                logger.error(f"EPG Source Error {source.name}: {e}")
                source.status = "Error"
            
            with Session(engine) as session:
                session.add(source); session.commit()

    # 生成 XML 字节流
    final_xml = etree.tostring(master_root, encoding="UTF-8", xml_declaration=True, pretty_print=True)
    state.epg_xml = final_xml
    state.epg_gz = gzip.compress(final_xml)
    logger.info(f"EPG 聚合完成，最终大小: {len(final_xml)//1024} KB")

# --- 4. 维护任务 ---
async def run_maintenance():
    if state.is_checking: return
    state.is_checking = True
    # 检测源存活... (复用之前的逻辑)
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
    
    # 执行 EPG 聚合
    await merge_epg_data()
    state.is_checking = False

async def maintenance_loop():
    while True:
        await run_maintenance()
        await asyncio.sleep(6 * 3600)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(maintenance_loop())

# --- 5. 路由 ---

@app.get("/epg.xml")
async def get_epg_xml():
    return Response(content=state.epg_xml, media_type="application/xml")

@app.get("/epg.xml.gz")
async def get_epg_gz():
    return Response(content=state.epg_gz, media_type="application/gzip")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
        epg_sources = session.exec(select(EPGSource)).all()
        epg_days = session.get(Setting, "epg_days").value
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "sources": sources, 
        "epg_sources": epg_sources,
        "epg_days": epg_days
    })

@app.post("/update_epg_settings")
async def update_epg_settings(request: Request, days: str = Form(...)):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        s = session.get(Setting, "epg_days")
        s.value = days
        session.add(s); session.commit()
    asyncio.create_task(run_maintenance())
    return RedirectResponse(url="/", status_code=303)

@app.post("/add_epg_source")
async def add_epg_source(request: Request, name: str = Form(...), url: str = Form(...)):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        session.add(EPGSource(name=name, url=url))
        session.commit()
    asyncio.create_task(run_maintenance())
    return RedirectResponse(url="/", status_code=303)

@app.get("/delete_epg/{eid}")
async def delete_epg(request: Request, eid: int):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        e = session.get(EPGSource, eid)
        if e: session.delete(e); session.commit()
    return RedirectResponse(url="/", status_code=303)

# ---------------------------------------------------------
# 此处下方粘贴之前的登录(/login)、注销(/logout)、监控API(/api/status)、
# 播放接口(/live/{name})、列表接口(/playlist.m3u) 等逻辑
# ---------------------------------------------------------

# (请确保 StreamPool 类定义和订阅逻辑也在其中)

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

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if is_authenticated(request): return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login(username: str = Form(...), password: str = Form(...)):
    if username == ADMIN_USER and password == ADMIN_PASS:
        resp = RedirectResponse(url="/", status_code=303)
        resp.set_cookie(key="session_id", value=SECRET_KEY, max_age=604800, httponly=True)
        return resp
    return RedirectResponse(url="/login?error=1", status_code=303)

@app.get("/logout")
async def logout():
    resp = RedirectResponse(url="/login", status_code=303)
    resp.delete_cookie("session_id"); return resp

@app.get("/api/status")
async def get_api_status(request: Request):
    if not is_authenticated(request): return {"active_streams": [], "is_checking": False}
    active = []
    for s_id, data in stream_pool.streams.items():
        active.append({"name": data["name"], "url": data["url"], "clients": list(data["clients"].values()), "client_count": len(data["clients"]), "speed": data["speed"], "info": data["info"]})
    return {"active_streams": active, "is_checking": state.is_checking}

@app.get("/live/{channel_name}")
async def proxy_live(request: Request, channel_name: str, url: str):
    client_ip = request.headers.get("x-real-ip") or request.client.host
    return StreamingResponse(stream_pool.subscribe(channel_name, url, client_ip), media_type="video/mp2t")

@app.get("/playlist.m3u")
@app.get("/playlist.txt")
async def get_playlist(request: Request, proxy: bool = False):
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", str(request.base_url.netloc))
    base_url = f"{scheme}://{host}"
    unique_channels, seen_urls = [], set()
    with Session(engine) as session: sources = session.exec(select(Source)).all()
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for s in sources:
            try:
                resp = await client.get(s.url); lines = resp.text.split('\n')
                for i in range(len(lines)):
                    line = lines[i].strip()
                    if line.startswith("#EXTINF"):
                        name = line.split(',')[-1].strip()
                        for j in range(i+1, min(i+5, len(lines))):
                            u = lines[j].strip()
                            if u.startswith("http") and u not in seen_urls:
                                unique_channels.append({"name": name, "url": u}); seen_urls.add(u); break
            except: continue
    if request.url.path.endswith('.txt'):
        lines = [f"{c['name']},{f'{base_url}/live/{c['name']}?
