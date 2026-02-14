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

# --- 1. 初始化与日志 ---
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

class Setting(SQLModel, table=True):
    key: str = Field(primary_key=True)
    value: str

SQLModel.metadata.create_all(engine)

# 数据库自动迁移与初始化设置
with engine.connect() as conn:
    try:
        inspector = conn.execute(text("PRAGMA table_info(source)"))
        columns = [row[1] for row in inspector]
        if "status" not in columns: conn.execute(text("ALTER TABLE source ADD COLUMN status VARCHAR DEFAULT 'Unknown'"))
        if "last_check" not in columns: conn.execute(text("ALTER TABLE source ADD COLUMN last_check FLOAT DEFAULT 0"))
        conn.commit()
        with Session(engine) as session:
            if not session.get(Setting, "epg_days"):
                session.add(Setting(key="epg_days", value="3"))
                session.commit()
    except Exception as e:
        logger.error(f"Migration error: {e}")

app = FastAPI()
templates = Jinja2Templates(directory="templates")
LOGO_BASE = "https://gcore.jsdelivr.net/gh/taksssss/tv/icon/"

class GlobalState:
    def __init__(self):
        self.epg_xml = b""
        self.epg_gz = b""
        self.is_checking = False

state = GlobalState()

# --- 2. 权限校验 ---
def is_authenticated(request: Request):
    return request.cookies.get("session_id") == SECRET_KEY

# --- 3. 频道清洗与分组 ---
def clean_channel_name(name: str) -> str:
    name = name.upper().replace(" ", "")
    patterns = [r"\(.*\)", r"（.*）", r"HD", r"高清", r"超清", r"4K", r"8K", r"蓝光", r"V2", r"V3", r"\(备用\)", r"\[.*\]", r"频道", r"-"]
    for p in patterns: name = re.sub(p, "", name)
    name = re.sub(r"CCTV(\d+)\+", r"CCTV\1+", name)
    return name.strip()

def get_auto_group(name: str, original_group: str = "") -> str:
    if original_group and original_group not in ["", "未分类", "聚合", "聚合频道"]: return original_group
    name = name.upper()
    if "CCTV" in name: return "央视频道"
    if "卫视" in name: return "卫视频道"
    if any(x in name for x in ["电影", "影院", "HBO", "剧场"]): return "电影频道"
    if any(x in name for x in ["体育", "足球", "篮球", "NBA", "CCTV5"]): return "体育频道"
    if any(x in name for x in ["少儿", "卡通", "动漫"]): return "少儿频道"
    if any(x in name for x in ["新闻", "资讯"]): return "新闻频道"
    if any(x in name for x in ["购物", "商城"]): return "购物频道"
    return "地方频道"

# --- 4. EPG 聚合引擎 ---
def parse_epg_time(t_str):
    try: return datetime.strptime(t_str[:14], "%Y%m%d%H%M%S")
    except: return None

async def merge_epg_data():
    logger.info("📡 开始聚合 EPG 数据...")
    with Session(engine) as session:
        epg_sources = session.exec(select(EPGSource)).all()
        days_limit = int(session.get(Setting, "epg_days").value)
    
    if not epg_sources:
        state.epg_xml = b'<?xml version="1.0" encoding="UTF-8"?><tv></tv>'
        state.epg_gz = gzip.compress(state.epg_xml)
        return

    master_root = etree.Element("tv")
    seen_channel_ids = set()
    cutoff_date = datetime.now() - timedelta(days=1)
    end_date = datetime.now() + timedelta(days=days_limit)

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        for source in epg_sources:
            try:
                r = await client.get(source.url)
                content = r.content
                if source.url.endswith(".gz") or content[:2] == b'\x1f\x8b':
                    content = gzip.decompress(content)
                root = etree.fromstring(content, parser=etree.XMLParser(recover=True))
                for channel in root.xpath("//channel"):
                    c_id = channel.get("id")
                    if c_id not in seen_channel_ids:
                        master_root.append(channel); seen_channel_ids.add(c_id)
                for prog in root.xpath("//programme"):
                    start = parse_epg_time(prog.get("start"))
                    if start and cutoff_date <= start <= end_date: master_root.append(prog)
                source.status = "Success"
            except Exception as e:
                logger.error(f"EPG Source Error {source.name}: {e}")
                source.status = "Error"
            with Session(engine) as session: session.add(source); session.commit()

    final_xml = etree.tostring(master_root, encoding="UTF-8", xml_declaration=True, pretty_print=True)
    state.epg_xml = final_xml
    state.epg_gz = gzip.compress(final_xml)

# --- 5. 缓冲池逻辑 (一拉多 & 客户端IP) ---
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
                                if stream_id in self.streams: self.streams[stream_id]["speed"] = f"{speed:.1f} KB/s"
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
        if stream_id in self.streams:
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

# --- 6. 维护巡检 ---
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
                s.last_check = time.time(); session.add(s)
            session.commit()
        await merge_epg_data()
    state.is_checking = False

async def maintenance_loop():
    while True: await run_maintenance(); await asyncio.sleep(6 * 3600)

@app.on_event("startup")
async def startup_event(): asyncio.create_task(maintenance_loop())

# --- 7. 基础路由 ---
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

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
        epg_sources = session.exec(select(EPGSource)).all()
        epg_days = session.get(Setting, "epg_days").value
    return templates.TemplateResponse("index.html", {"request": request, "sources": sources, "epg_sources": epg_sources, "epg_days": epg_days})

# --- 8. 核心输出接口 (M3U/TXT/EPG) ---
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
                if s.type == 'm3u':
                    for i in range(len(lines)):
                        line = lines[i].strip()
                        if line.startswith("#EXTINF"):
                            g = re.search(r'group-title="(.*?)"', line)
                            raw_name = line.split(',')[-1].strip()
                            clean_name = clean_channel_name(raw_name)
                            for j in range(i+1, min(i+5, len(lines))):
                                u = lines[j].strip()
                                if u.startswith("http") and u not in seen_urls:
                                    unique_channels.append({"name": raw_name, "clean": clean_name, "url": u, "group": get_auto_group(clean_name, g.group(1) if g else "")})
                                    seen_urls.add(u); break
                else:
                    for line in lines:
                        if ',' in line:
                            name, url = line.split(',', 1); u = url.strip()
                            if u not in seen_urls:
                                clean_name = clean_channel_name(name.strip())
                                unique_channels.append({"name": name.strip(), "clean": clean_name, "url": u, "group": get_auto_group(clean_name)}); seen_urls.add(u)
            except: continue
    
    unique_channels.sort(key=lambda x: (x['group'], x['clean']))
    is_txt = request.url.path.endswith('.txt')
    if is_txt:
        lines, current_group = [], ""
        for c in unique_channels:
            if c['group'] != current_group:
                current_group = c['group']
                lines.append(f"{current_group},#genre#")
            quoted_url = urllib.parse.quote(c['url'], safe='')
            final_url = f"{base_url}/live/{c['name']}?url={quoted_url}" if proxy else c['url']
            lines.append(f"{c['name']},{final_url}")
        return Response(content="\n".join(lines), media_type="text/plain")
    else:
        output = f'#EXTM3U x-tvg-url="{base_url}/epg.xml.gz"\n'
        for c in unique_channels:
            logo = f"{LOGO_BASE}{c['clean']}.png"
            quoted_url = urllib.parse.quote(c['url'], safe='')
            final_url = f"{base_url}/live/{c['name']}?url={quoted_url}" if proxy else c['url']
            output += f'#EXTINF:-1 tvg-name="{c["name"]}" tvg-logo="{logo}" group-title="{c["group"]}",{c["name"]}\n{final_url}\n'
        return Response(content=output, media_type="application/x-mpegurl")

@app.get("/live/{channel_name}")
async def proxy_live(request: Request, channel_name: str, url: str):
    client_ip = request.headers.get("x-real-ip") or request.client.host
    return StreamingResponse(stream_pool.subscribe(channel_name, url, client_ip), media_type="video/mp2t")

@app.get("/epg.xml")
async def get_epg():
    if not state.epg_xml: return Response(content='<?xml version="1.0" encoding="UTF-8"?><tv></tv>', media_type="application/xml")
    return Response(content=state.epg_xml, media_type="application/xml")

@app.get("/epg.xml.gz")
async def get_epg_gz():
    if not state.epg_gz: 
        empty_gz = gzip.compress(b'<?xml version="1.0" encoding="UTF-8"?><tv></tv>')
        return Response(content=empty_gz, media_type="application/gzip")
    return Response(content=state.epg_gz, media_type="application/gzip")

# --- 9. 操作接口 ---
@app.get("/api/status")
async def get_api_status(request: Request):
    if not is_authenticated(request): return {"active_streams": [], "is_checking": False}
    active = []
    for s_id, data in stream_pool.streams.items():
        active.append({"name": data["name"], "url": data["url"], "clients": list(data["clients"].values()), "client_count": len(data["clients"]), "speed": data["speed"], "info": data["info"]})
    return {"active_streams": active, "is_checking": state.is_checking}

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

@app.post("/add_epg_source")
async def add_epg_source(request: Request, name: str = Form(...), url: str = Form(...)):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session: session.add(EPGSource(name=name, url=url)); session.commit()
    asyncio.create_task(run_maintenance()); return RedirectResponse(url="/", status_code=303)

@app.get("/delete_epg/{eid}")
async def delete_epg(request: Request, eid: int):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        e = session.get(EPGSource, eid)
        if e: session.delete(e); session.commit()
    return RedirectResponse(url="/", status_code=303)

@app.post("/update_epg_settings")
async def update_epg_settings(request: Request, days: str = Form(...)):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        s = session.get(Setting, "epg_days")
        s.value = days; session.add(s); session.commit()
    asyncio.create_task(run_maintenance()); return RedirectResponse(url="/", status_code=303)

@app.get("/refresh")
async def manual_refresh(request: Request):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    asyncio.create_task(run_maintenance()); return RedirectResponse(url="/", status_code=303)
