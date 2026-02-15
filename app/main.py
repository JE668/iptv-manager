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
from collections import deque

import httpx
from fastapi import FastAPI, Form, Request, Response, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Field, Session, SQLModel, create_engine, select
from sqlalchemy import text
from lxml import etree

# --- 1. 配置与日志 ---
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
    name: str; url: str; type: str; status: str = "Unknown"; last_check: float = 0

class EPGSource(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str; url: str; status: str = "Unknown"

class Setting(SQLModel, table=True):
    key: str = Field(primary_key=True); value: str

SQLModel.metadata.create_all(engine)

with Session(engine) as session:
    if not session.get(Setting, "epg_days"): session.add(Setting(key="epg_days", value="3"))
    if not session.get(Setting, "proxy_mode"): session.add(Setting(key="proxy_mode", value="1"))
    if not session.get(Setting, "epg_interval"): session.add(Setting(key="epg_interval", value="6"))
    session.commit()

app = FastAPI()
templates = Jinja2Templates(directory="templates")
LOGO_BASE = "https://gcore.jsdelivr.net/gh/taksssss/tv/icon/"

class GlobalState:
    def __init__(self):
        self.epg_xml = b""; self.epg_gz = b""; self.last_epg_update = 0; self.last_playlist_request = 0
        self.is_epg_updating = False; self.epg_logs = []
state = GlobalState()

def is_authenticated(request: Request): return request.cookies.get("session_id") == SECRET_KEY

# --- 2. 蓄水池流控引擎 ---

class StreamPool:
    def __init__(self):
        self.streams: Dict = {}

    async def get_stream_info(self, url: str):
        try:
            cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-select_streams', 'v:0', '-analyzeduration', '3000000', url]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10.0)
            data = json.loads(stdout)
            if 'streams' in data and data['streams']:
                s = data['streams'][0]
                return {"res": f"{s.get('width')}x{s.get('height')}", "codec": s.get('codec_name', '未知').upper()}
        except: pass
        return {"res": "未知", "codec": "未知"}

    async def _fetcher(self, stream_id: str, url: str, name: str):
        logger.info(f"🚀 [中继开启] {name}")
        while stream_id in self.streams:
            if not self.streams[stream_id]["clients"]:
                await asyncio.sleep(8) # 延长无人观看的释放时间到8秒
                if not self.streams[stream_id]["clients"]:
                    logger.info(f"👋 [释放中继] {name}")
                    break
            
            try:
                if self.streams[stream_id]["info"]["res"] == "探测中...":
                    self.streams[stream_id]["info"] = await self.get_stream_info(url)

                # 增加缓冲区读取深度
                async with httpx.AsyncClient(timeout=httpx.Timeout(20.0, read=20.0), follow_redirects=True) as client:
                    async with client.stream("GET", url) as r:
                        if r.status_code != 200: raise Exception(f"HTTP {r.status_code}")
                        
                        start_time, bytes_in = time.time(), 0
                        async for chunk in r.aiter_bytes(chunk_size=128*1024):
                            if not self.streams[stream_id]["clients"]: return
                            
                            # 核心：将块存入“蓄水池”队列
                            self.streams[stream_id]["history"].append(chunk)
                            
                            bytes_in += len(chunk)
                            now = time.time()
                            if now - start_time >= 1.0:
                                self.streams[stream_id]["in_speed"] = f"{(bytes_in/1024/(now-start_time)):.1f} KB/s"
                                bytes_in, start_time = 0, now
                            
                            # 分发给所有当前连接的观众
                            for q in self.streams[stream_id]["queues"]:
                                try: q.put_nowait(chunk)
                                except asyncio.QueueFull: pass
            except Exception as e:
                logger.warning(f"⚠️ [重连中] {name}: {e}")
                if stream_id in self.streams: self.streams[stream_id]["in_speed"] = "重连中..."
                await asyncio.sleep(2)
        self.streams.pop(stream_id, None)

    async def subscribe(self, name: str, url: str, client_ip: str):
        stream_id = hashlib.md5(url.encode()).hexdigest()
        client_id = hashlib.md5(f"{client_ip}{time.time()}".encode()).hexdigest()[:8]
        
        if stream_id not in self.streams:
            self.streams[stream_id] = {
                "name": name, "url": url, "queues": [], "clients": {}, 
                "info": {"res": "探测中...", "codec": "探测中..."},
                "in_speed": "0 KB/s", "out_speed": "0 KB/s",
                "history": deque(maxlen=40) # 蓄水池：存储最近约 5MB 的历史块
            }
            asyncio.create_task(self._fetcher(stream_id, url, name))
            await asyncio.sleep(0.8) # 给 fetcher 足够的时间先去“蓄水”

        # 观众专属队列
        queue = asyncio.Queue(maxsize=200) # 加大下游队列深度到200
        
        # 核心：新观众加入时，先“瞬间灌满”他的缓冲区
        if stream_id in self.streams:
            for old_chunk in list(self.streams[stream_id]["history"]):
                await queue.put(old_chunk)
            
            self.streams[stream_id]["queues"].append(queue)
            self.streams[stream_id]["clients"][client_id] = {"ip": client_ip, "out_bytes": 0, "speed": "0 KB/s", "last_ts": time.time()}
        
        try:
            while True:
                chunk = await queue.get()
                if stream_id not in self.streams: break
                
                c = self.streams[stream_id]["clients"].get(client_id)
                if c:
                    c["out_bytes"] += len(chunk)
                    now = time.time()
                    if now - c["last_ts"] >= 1.0:
                        c["speed"] = f"{(c['out_bytes']/1024/(now-c['last_ts'])):.1f} KB/s"
                        c["out_bytes"], c["last_ts"] = 0, now
                        total_out = sum([float(cli["speed"].split(' ')[0]) for cli in self.streams[stream_id]["clients"].values()])
                        self.streams[stream_id]["out_speed"] = f"{total_out:.1f} KB/s"
                
                # 缓冲区百分比计算 (qsize / maxsize)
                self.streams[stream_id]["buffer_level"] = int((queue.qsize() / 200) * 100)
                yield chunk
        finally:
            if stream_id in self.streams:
                self.streams[stream_id]["queues"].remove(queue)
                self.streams[stream_id]["clients"].pop(client_id, None)

stream_pool = StreamPool()

# --- 3. EPG & 订阅逻辑 (保持稳定) ---

async def update_epg_task():
    if state.is_epg_updating: return
    state.is_epg_updating = True
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    state.epg_logs.insert(0, f"[{ts}] ⏳ 启动聚合...")
    master_root = etree.Element("tv")
    with Session(engine) as session:
        eps = session.exec(select(EPGSource)).all()
        days_limit = int(session.get(Setting, "epg_days").value)
    cutoff, end = datetime.now()-timedelta(days=1), datetime.now()+timedelta(days=days_limit)
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        for s in eps:
            try:
                r = await client.get(s.url); content = r.content
                if s.url.endswith(".gz") or content[:2] == b'\x1f\x8b': content = gzip.decompress(content)
                root = etree.fromstring(content, parser=etree.XMLParser(recover=True))
                channels, progs = root.xpath("//channel"), root.xpath("//programme")
                v_progs = 0
                for c in channels: master_root.append(c)
                for p in progs:
                    try:
                        st = datetime.strptime(p.get("start")[:14], "%Y%m%d%H%M%S")
                        if cutoff <= st <= end: master_root.append(p); v_progs += 1
                    except: pass
                s.status = "Success"; state.epg_logs.insert(0, f"[{ts}] ✅ {s.name}: 导入 {len(channels)}频道, {v_progs}节目")
            except Exception as e:
                s.status = "Error"; state.epg_logs.insert(0, f"[{ts}] ❌ {s.name}: {str(e)}")
            with Session(engine) as session: session.add(s); session.commit()
    final = etree.tostring(master_root, encoding="UTF-8", xml_declaration=True, pretty_print=True)
    state.epg_xml, state.epg_gz = final, gzip.compress(final)
    state.last_epg_update, state.is_epg_updating = time.time(), False

async def epg_loop():
    while True:
        with Session(engine) as session: interval = int(session.get(Setting, "epg_interval").value)
        await update_epg_task(); await asyncio.sleep(interval * 3600)

@app.on_event("startup")
async def startup():
    asyncio.create_task(epg_loop())

async def fetch_realtime_sources(force_proxy: bool = False):
    unique_channels, seen_urls = [], set()
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
        p_mode = int(session.get(Setting, "proxy_mode").value)
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        tasks = [client.get(s.url) for s in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, resp in enumerate(results):
            if isinstance(resp, Exception): continue
            lines = resp.text.split('\n')
            for j in range(len(lines)):
                line = lines[j].strip()
                if line.startswith("#EXTINF"):
                    name = line.split(',')[-1].strip()
                    for k in range(j+1, min(j+5, len(lines))):
                        u = lines[k].strip()
                        if u.startswith("http") and u not in seen_urls:
                            use_proxy = force_proxy or (p_mode == 2) or (p_mode == 1 and ("/udp/" in u or "/rtp/" in u))
                            unique_channels.append({"name": name, "url": u, "use_proxy": use_proxy}); seen_urls.add(u); break
                elif ',' in line and not line.startswith("#"):
                    parts = line.split(',', 1)
                    name, u = parts[0].strip(), parts[1].strip()
                    if u not in seen_urls:
                        use_proxy = force_proxy or (p_mode == 2) or (p_mode == 1 and ("/udp/" in u or "/rtp/" in u))
                        unique_channels.append({"name": name, "url": u, "use_proxy": use_proxy}); seen_urls.add(u)
    return unique_channels

# --- 4. 路由 ---

@app.get("/playlist.m3u")
@app.get("/playlist.txt")
async def get_playlist(request: Request, proxy: bool = False):
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", str(request.base_url.netloc))
    base_url = f"{scheme}://{host}"
    channels = await fetch_realtime_sources(force_proxy=proxy)
    state.last_playlist_request = time.time()
    if request.url.path.endswith('.txt'):
        lines = []
        for c in channels:
            q_url = urllib.parse.quote(c['url'], safe='')
            p_url = f"{base_url}/live/{c['name']}?url={q_url}" if c['use_proxy'] else c['url']
            lines.append(f"{c['name']},{p_url}")
        return Response(content="\n".join(lines), media_type="text/plain")
    output = f'#EXTM3U x-tvg-url="{base_url}/epg.xml.gz"\n'
    for c in channels:
        q_url = urllib.parse.quote(c['url'], safe='')
        p_url = f"{base_url}/live/{c['name']}?url={q_url}" if c['use_proxy'] else c['url']
        output += f'#EXTINF:-1 tvg-logo="{LOGO_BASE}{c["name"].upper()}.png",{c["name"]}\n{p_url}\n'
    return Response(content=output, media_type="application/x-mpegurl")

@app.get("/live/{channel_name}")
async def proxy_live(request: Request, channel_name: str, url: str):
    client_ip = request.headers.get("x-real-ip") or request.client.host
    headers = {"X-Accel-Buffering": "no", "Cache-Control": "no-cache"}
    return StreamingResponse(stream_pool.subscribe(channel_name, url, client_ip), media_type="video/mp2t", headers=headers)

@app.get("/api/status")
async def api_status():
    active = []
    t_in, t_out, t_peers = 0, 0, 0
    for s_id, d in stream_pool.streams.items():
        try: t_in += float(d["in_speed"].split(' ')[0]); t_out += float(d["out_speed"].split(' ')[0])
        except: pass
        t_peers += len(d["clients"])
        active.append({"name": d["name"], "url": d["url"], "in_speed": d["in_speed"], "out_speed": d["out_speed"], "peers": len(d["clients"]), "info": d["info"], "buffer": f"{d['buffer_level']}/100", "clients": list(d["clients"].values())})
    return {"active_streams": active, "is_checking": state.is_epg_updating, "last_epg": time.strftime("%H:%M:%S", time.localtime(state.last_epg_update)) if state.last_epg_update else "从未更新", "last_m3u": time.strftime("%H:%M:%S", time.localtime(state.last_playlist_request)) if state.last_playlist_request else "从无请求", "kpis": {"total_in": f"{t_in:.1f} KB/s", "total_out": f"{t_out:.1f} KB/s", "stream_count": len(active), "peer_count": t_peers}, "epg_logs": state.epg_logs}

# 其余管理路由保持不变 (index, login, logout, add/delete)
@app.get("/")
async def index(request: Request):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session: return templates.TemplateResponse("index.html", {"request": request, "sources": session.exec(select(Source)).all(), "epg_sources": session.exec(select(EPGSource)).all(), "epg_days": session.get(Setting, "epg_days").value, "proxy_mode": session.get(Setting, "proxy_mode").value})
@app.get("/login", response_class=HTMLResponse)
async def l_p(request: Request): return templates.TemplateResponse("login.html", {"request": request})
@app.post("/login")
async def l_post(username: str = Form(...), password: str = Form(...)):
    if username == ADMIN_USER and password == ADMIN_PASS:
        r = RedirectResponse(url="/", status_code=303); r.set_cookie(key="session_id", value=SECRET_KEY, max_age=604800, httponly=True); return r
    return RedirectResponse(url="/login?error=1", status_code=303)
@app.get("/logout")
async def l_out(): r = RedirectResponse(url="/login", status_code=303); r.delete_cookie("session_id"); return r
@app.get("/epg.xml")
async def g_epg(): return Response(content=state.epg_xml, media_type="application/xml")
@app.get("/epg.xml.gz")
async def g_epgz(): return Response(content=state.epg_gz, media_type="application/gzip")
@app.post("/add_source")
async def a_s(request: Request, name: str = Form(...), url: str = Form(...), type: str = Form(...)):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session: session.add(Source(name=name, url=url, type=type)); session.commit()
    return RedirectResponse(url="/", status_code=303)
@app.get("/delete/{sid}")
async def d_s(sid: int):
    with Session(engine) as session:
        s = session.get(Source, sid); 
        if s: session.delete(s); session.commit()
    return RedirectResponse(url="/", status_code=303)
@app.post("/add_epg_source")
async def a_e(name: str = Form(...), url: str = Form(...)):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session: session.add(EPGSource(name=name, url=url)); session.commit()
    asyncio.create_task(update_epg_task()); return RedirectResponse(url="/", status_code=303)
@app.get("/delete_epg/{eid}")
async def d_e(eid: int):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        e = session.get(EPGSource, eid);
        if e: session.delete(e); session.commit()
    return RedirectResponse(url="/", status_code=303)
@app.post("/update_global_settings")
async def u_g(proxy_mode: str = Form(...), epg_days: str = Form(...)):
    with Session(engine) as session:
        pm = session.get(Setting, "proxy_mode"); pm.value = proxy_mode; session.add(pm)
        ed = session.get(Setting, "epg_days"); ed.value = epg_days; session.add(ed)
        session.commit()
    return RedirectResponse(url="/", status_code=303)
@app.get("/refresh")
async def ref(): asyncio.create_task(update_epg_task()); return RedirectResponse(url="/", status_code=303)
