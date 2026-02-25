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

def get_setting(session: Session, key: str, default_val: str) -> str:
    obj = session.get(Setting, key)
    return obj.value if obj else default_val

# 数据库自动迁移
with engine.connect() as conn:
    try:
        inspector = conn.execute(text("PRAGMA table_info(source)"))
        columns = [row[1] for row in inspector]
        if "status" not in columns: conn.execute(text("ALTER TABLE source ADD COLUMN status VARCHAR DEFAULT 'Unknown'"))
        if "last_check" not in columns: conn.execute(text("ALTER TABLE source ADD COLUMN last_check FLOAT DEFAULT 0"))
        conn.commit()
    except: pass

with Session(engine) as session:
    try:
        if not session.get(Setting, "epg_days"): session.add(Setting(key="epg_days", value="3"))
        if not session.get(Setting, "proxy_mode"): session.add(Setting(key="proxy_mode", value="1"))
        if not session.get(Setting, "epg_interval"): session.add(Setting(key="epg_interval", value="6"))
        if not session.get(Setting, "buffer_threshold"): session.add(Setting(key="buffer_threshold", value="15"))
        if not session.get(Setting, "buffer_timeout"): session.add(Setting(key="buffer_timeout", value="3.0"))
        if not session.get(Setting, "access_token"): session.add(Setting(key="access_token", value="")) # 默认无Token
        session.commit()
    except: pass

app = FastAPI()
templates = Jinja2Templates(directory="templates")
LOGO_BASE = "https://taksssss.github.io/tv/icon/"

class GlobalState:
    def __init__(self):
        self.epg_xml = b""; self.epg_gz = b""; self.last_epg_update = 0; self.last_playlist_request = 0
        self.is_epg_updating = False; self.epg_logs =[]
        self.alias_map = {}; self.regex_aliases = []; self.categories =[]; self.channel_to_category = {}
state = GlobalState()

# --- 2. 权限与Token校验 ---
def is_authenticated(request: Request):
    return request.cookies.get("session_id") == SECRET_KEY

def verify_token(request: Request):
    """验证防盗链 Token"""
    with Session(engine) as session:
        token = get_setting(session, "access_token", "")
    if token:
        req_token = request.query_params.get("token", "")
        if req_token != token:
            raise HTTPException(status_code=403, detail="Forbidden: Invalid Token")

# --- 3. 别名与分类引擎 ---
def load_alias_and_demo():
    state.alias_map, state.regex_aliases = {},[]
    if os.path.exists("alias.txt"):
        with open("alias.txt", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"): continue
                parts = line.split(",")
                main_name = parts[0].strip()
                for a in parts[1:]:
                    a = a.strip()
                    if a.startswith("re:"):
                        try: state.regex_aliases.append((re.compile(a[3:]), main_name))
                        except: pass
                    else: state.alias_map[a.upper()] = main_name

    state.categories, state.channel_to_category =[], {}
    if os.path.exists("demo.txt"):
        cur_cat = None
        with open("demo.txt", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                if ",#genre#" in line:
                    cur_cat = {"name": line.split(",")[0].strip(), "channels":[]}
                    state.categories.append(cur_cat)
                elif cur_cat:
                    ch = line.strip()
                    cur_cat["channels"].append(ch)
                    state.channel_to_category[ch] = cur_cat["name"]

def get_standard_name(raw_name: str) -> str:
    name_up = raw_name.strip().upper()
    if name_up in state.alias_map: return state.alias_map[name_up]
    for pattern, main in state.regex_aliases:
        if pattern.search(raw_name): return main
    clean = raw_name.upper().replace(" ", "")
    for p in[r"\(.*\)", r"（.*）", r"HD", r"高清", r"超清", r"4K", r"8K", r"蓝光", r"V\d", r"\(备用\)", r"\[.*\]", r"频道", r"-"]:
        clean = re.sub(p, "", clean)
    return clean.strip()

# --- 4. 蓄水池流控引擎 ---
class StreamPool:
    def __init__(self):
        self.streams: Dict = {}

    async def get_stream_info(self, url: str):
        try:
            cmd =['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-select_streams', 'v:0', '-analyzeduration', '5000000', '-probesize', '5000000', url]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=12.0)
            data = json.loads(stdout)
            if 'streams' in data and data['streams']:
                s = data['streams'][0]
                return {"res": f"{s.get('width')}x{s.get('height')}", "codec": s.get('codec_name', '未知').upper()}
        except: pass
        return {"res": "未知", "codec": "未知"}

    async def _fetcher(self, stream_id: str, url: str, name: str):
        logger.info(f"🚀 [中继启动] {name}")
        while stream_id in self.streams:
            if not self.streams[stream_id]["clients"]:
                await asyncio.sleep(8)
                if not self.streams[stream_id]["clients"]: break
            try:
                if self.streams[stream_id]["info"]["res"] == "探测中...":
                    self.streams[stream_id]["info"] = await self.get_stream_info(url)
                
                async with httpx.AsyncClient(timeout=httpx.Timeout(20.0, read=20.0), follow_redirects=True) as client:
                    async with client.stream("GET", url) as r:
                        if r.status_code != 200: raise Exception(f"HTTP {r.status_code}")
                        start_time, bytes_in = time.time(), 0
                        async for chunk in r.aiter_bytes(chunk_size=128*1024):
                            if not self.streams[stream_id]["clients"]: return
                            self.streams[stream_id]["history"].append(chunk)
                            bytes_in += len(chunk); now = time.time()
                            if now - start_time >= 1.0:
                                self.streams[stream_id]["in_speed"] = f"{(bytes_in/1024/(now-start_time)):.1f} KB/s"
                                bytes_in, start_time = 0, now
                            for q in self.streams[stream_id]["queues"]:
                                try: q.put_nowait(chunk)
                                except asyncio.QueueFull: pass
            except Exception as e:
                logger.warning(f"⚠️[中继重连] {name}: {e}")
                if stream_id in self.streams: self.streams[stream_id]["in_speed"] = "重连中..."
                await asyncio.sleep(3)
        self.streams.pop(stream_id, None)

    async def subscribe(self, name: str, url: str, client_ip: str):
        stream_id = hashlib.md5(url.encode()).hexdigest()
        client_id = hashlib.md5(f"{client_ip}{time.time()}".encode()).hexdigest()[:8]
        
        if stream_id not in self.streams:
            self.streams[stream_id] = {
                "name": name, "url": url, "queues":[], "clients": {}, 
                "info": {"res": "探测中...", "codec": "探测中..."},
                "in_speed": "0 KB/s", "out_speed": "0 KB/s", "buffer_level": 0, "history": deque(maxlen=60)
            }
            asyncio.create_task(self._fetcher(stream_id, url, name))
            await asyncio.sleep(0.5)

        queue = asyncio.Queue(maxsize=300)
        
        if stream_id in self.streams:
            # 发送历史缓存，实现秒开和预缓冲
            for old in list(self.streams[stream_id]["history"]): await queue.put(old)
            self.streams[stream_id]["queues"].append(queue)
            self.streams[stream_id]["clients"][client_id] = {"ip": client_ip, "out_bytes": 0, "speed": "0 KB/s", "last_ts": time.time()}

        with Session(engine) as session:
            buf_thresh = int(get_setting(session, "buffer_threshold", "15"))
            buf_timeout = float(get_setting(session, "buffer_timeout", "3.0"))
            
        wait_start = time.time()
        while queue.qsize() < buf_thresh and (time.time() - wait_start) < buf_timeout:
            if stream_id not in self.streams: break
            await asyncio.sleep(0.1)

        try:
            while True:
                chunk = await queue.get()
                if stream_id not in self.streams: break
                c = self.streams[stream_id]["clients"].get(client_id)
                if c:
                    c["out_bytes"] += len(chunk); now = time.time()
                    if now - c["last_ts"] >= 1.0:
                        c["speed"] = f"{(c['out_bytes']/1024/(now-c['last_ts'])):.1f} KB/s"
                        c["out_bytes"], c["last_ts"] = 0, now
                        total_out = sum([float(cli["speed"].split(' ')[0]) for cli in self.streams[stream_id]["clients"].values()])
                        self.streams[stream_id]["out_speed"] = f"{total_out:.1f} KB/s"
                
                # 修改：前端显示的 buffer_level 改为 history 的蓄水比例 (直观看到是否健康)
                hist_len = len(self.streams[stream_id]["history"])
                self.streams[stream_id]["buffer_level"] = int((hist_len / 60) * 100)
                yield chunk
        finally:
            if stream_id in self.streams:
                self.streams[stream_id]["queues"].remove(queue)
                self.streams[stream_id]["clients"].pop(client_id, None)
                if not self.streams[stream_id]["clients"]:
                    self.streams[stream_id]["out_speed"] = "0 KB/s"
                    self.streams[stream_id]["in_speed"] = "0 KB/s"

stream_pool = StreamPool()

# --- 5. EPG 与 自动测活引擎 ---
async def update_epg_task():
    if state.is_epg_updating: return
    state.is_epg_updating = True; load_alias_and_demo()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    state.epg_logs.insert(0, f"[{ts}] ⏳ 启动聚合...")
    master = etree.Element("tv")
    with Session(engine) as session:
        eps = session.exec(select(EPGSource)).all()
        days_limit = int(get_setting(session, "epg_days", "3"))
    cutoff, end = datetime.now()-timedelta(days=1), datetime.now()+timedelta(days=days_limit)
    
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        for s in eps:
            try:
                r = await client.get(s.url); content = r.content
                if s.url.endswith(".gz") or content[:2] == b'\x1f\x8b': content = gzip.decompress(content)
                root = etree.fromstring(content, parser=etree.XMLParser(recover=True))
                channels, progs = root.xpath("//channel"), root.xpath("//programme")
                v_progs = 0
                for c in channels: master.append(c)
                for p in progs:
                    try:
                        st = datetime.strptime(p.get("start")[:14], "%Y%m%d%H%M%S")
                        if cutoff <= st <= end: master.append(p); v_progs += 1
                    except: pass
                s.status = "Success"; state.epg_logs.insert(0, f"[{ts}] ✅ {s.name}: 导入 {len(channels)}频道, {v_progs}节目")
            except Exception as e:
                s.status = "Error"; state.epg_logs.insert(0, f"[{ts}] ❌ {s.name}: {str(e)}")
            with Session(engine) as session: session.add(s); session.commit()
            
    final = etree.tostring(master, encoding="UTF-8", xml_declaration=True, pretty_print=True)
    state.epg_xml, state.epg_gz = final, gzip.compress(final)
    state.last_epg_update, state.is_epg_updating = time.time(), False

@app.on_event("startup")
async def startup():
    load_alias_and_demo()
    asyncio.create_task(update_epg_task())
    async def loop():
        while True:
            with Session(engine) as session: interval = int(get_setting(session, "epg_interval", "6"))
            await asyncio.sleep(interval * 3600); await update_epg_task()
    asyncio.create_task(loop())

async def fetch_realtime_sources(force_proxy: bool = False):
    unique_channels, seen_urls =[], set()
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
        p_mode = int(get_setting(session, "proxy_mode", "1"))
        
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        tasks = [client.get(s.url) for s in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 将检测结果写回数据库，解决“检测失效”问题
        with Session(engine) as db_session:
            for i, resp in enumerate(results):
                s_db = db_session.get(Source, sources[i].id)
                s_db.last_check = time.time()
                if isinstance(resp, Exception) or getattr(resp, 'status_code', 500) >= 400:
                    s_db.status = "Offline"; db_session.add(s_db); continue
                
                s_db.status = "Online"; db_session.add(s_db)
                lines = resp.text.split('\n')
                if sources[i].type == 'm3u':
                    cur_name = None
                    for line in lines:
                        line = line.strip()
                        if line.startswith("#EXTINF"): cur_name = line.split(',')[-1].strip()
                        elif cur_name and not line.startswith("#"):
                            u = line
                            if u not in seen_urls:
                                use_p = force_proxy or (p_mode == 2) or (p_mode == 1 and ("/udp/" in u or "/rtp/" in u))
                                unique_channels.append({"name": cur_name, "url": u, "use_proxy": use_p}); seen_urls.add(u)
                            cur_name = None
                else:
                    for line in lines:
                        if ',' in line and not line.startswith("#"):
                            parts = line.split(',', 1)
                            if len(parts) == 2:
                                name, u = parts[0].strip(), parts[1].strip()
                                if u and u not in seen_urls:
                                    use_p = force_proxy or (p_mode == 2) or (p_mode == 1 and ("/udp/" in u or "/rtp/" in u))
                                    unique_channels.append({"name": name, "url": u, "use_proxy": use_p}); seen_urls.add(u)
            db_session.commit()
    return unique_channels

# --- 6. 路由分发 ---
@app.get("/api/status")
async def api_status():
    active, total_in, total_out, total_peers =[], 0, 0, 0
    for s_id, d in list(stream_pool.streams.items()):
        num_c = len(d["clients"])
        if num_c > 0:
            try: total_in += float(d["in_speed"].split(' ')[0]); total_out += float(d["out_speed"].split(' ')[0])
            except: pass
            total_peers += num_c
        active.append({"name": d["name"], "url": d["url"], "in_speed": d["in_speed"], "out_speed": d["out_speed"], "peers": num_c, "info": d["info"], "buffer": f"{d.get('buffer_level', 0)}/100", "clients": list(d["clients"].values())})
    return {"active_streams": active, "is_checking": state.is_epg_updating, "last_epg": time.strftime("%H:%M:%S", time.localtime(state.last_epg_update)) if state.last_epg_update else "待同步", "last_m3u": time.strftime("%H:%M:%S", time.localtime(state.last_playlist_request)) if state.last_playlist_request else "无请求", "kpis": {"total_in": f"{total_in:.1f} KB/s", "total_out": f"{total_out:.1f} KB/s", "stream_count": len([x for x in active if x['peers']>0]), "peer_count": total_peers}, "epg_logs": state.epg_logs}

@app.get("/playlist.m3u")
@app.get("/playlist.txt")
async def get_playlist(request: Request, proxy: bool = False):
    verify_token(request) # 防盗链校验
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", str(request.base_url.netloc))
    base_url = f"{scheme}://{host}"
    
    with Session(engine) as session: token = get_setting(session, "access_token", "")
    token_query = f"&token={token}" if token else ""
    
    raw_channels = await fetch_realtime_sources(force_proxy=proxy)
    unique_channels =[]
    
    for c in raw_channels:
        std_name = get_standard_name(c['name'])
        cat = state.channel_to_category.get(std_name, "其他频道")
        unique_channels.append({"name": std_name, "url": c['url'], "group": cat, "use_proxy": c['use_proxy']})
        
    cat_order = {cat['name']: i for i, cat in enumerate(state.categories)}; cat_order["其他频道"] = 999
    def sort_key(ch):
        cw = cat_order.get(ch['group'], 998); chw = 999
        if ch['group'] != "其他频道":
            for i, n in enumerate(state.categories[cw]['channels']):
                if n == ch['name']: chw = i; break
        return (cw, chw, ch['name'])
    
    unique_channels.sort(key=sort_key)
    state.last_playlist_request = time.time()

    if request.url.path.endswith('.txt'):
        txt_lines, cur_cat =[], ""
        for c in unique_channels:
            if c['group'] != cur_cat: cur_cat = c['group']; txt_lines.append(f"{cur_cat},#genre#")
            c_name = c['name']; c_url = c['url']
            p_url = f"{base_url}/live/{c_name}?url={urllib.parse.quote(c_url, safe='')}{token_query}" if c['use_proxy'] else c_url
            txt_lines.append(f"{c_name},{p_url}")
        return Response(content="\n".join(txt_lines), media_type="text/plain")
    
    # EPG 链接也加上 token
    epg_url = f"{base_url}/epg.xml.gz?token={token}" if token else f"{base_url}/epg.xml.gz"
    output = f'#EXTM3U x-tvg-url="{epg_url}"\n'
    for c in unique_channels:
        c_name = c['name']; c_url = c['url']
        logo = f"{LOGO_BASE}{c_name}.png"
        p_url = f"{base_url}/live/{c_name}?url={urllib.parse.quote(c_url, safe='')}{token_query}" if c['use_proxy'] else c_url
        output += f'#EXTINF:-1 tvg-logo="{logo}" group-title="{c["group"]}",{c_name}\n{p_url}\n'
    return Response(content=output, media_type="application/x-mpegurl")

@app.get("/")
async def index(request: Request):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session: 
        return templates.TemplateResponse("index.html", {
            "request": request, "sources": session.exec(select(Source)).all(), 
            "epg_sources": session.exec(select(EPGSource)).all(), 
            "epg_days": get_setting(session, "epg_days", "3"), 
            "proxy_mode": get_setting(session, "proxy_mode", "1"),
            "buffer_threshold": get_setting(session, "buffer_threshold", "15"),
            "buffer_timeout": get_setting(session, "buffer_timeout", "3.0"),
            "access_token": get_setting(session, "access_token", "")
        })

@app.get("/check_sources")
async def check_sources_route(request: Request):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    await fetch_realtime_sources() 
    return RedirectResponse(url="/", status_code=303)

@app.post("/update_global_settings")
async def u_g(request: Request, proxy_mode: str = Form(...), epg_days: str = Form(...), buffer_threshold: str = Form(...), buffer_timeout: str = Form(...), access_token: str = Form(default="")):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        for k, v in[("proxy_mode", proxy_mode), ("epg_days", epg_days), ("buffer_threshold", buffer_threshold), ("buffer_timeout", buffer_timeout), ("access_token", access_token)]:
            obj = session.get(Setting, k)
            if obj: obj.value = str(v)
            else: session.add(Setting(key=k, value=str(v)))
        session.commit()
    return RedirectResponse(url="/", status_code=303)

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request): return templates.TemplateResponse("login.html", {"request": request})
@app.post("/login")
async def login_post(username: str = Form(...), password: str = Form(...)):
    if username == ADMIN_USER and password == ADMIN_PASS:
        r = RedirectResponse(url="/", status_code=303); r.set_cookie(key="session_id", value=SECRET_KEY, max_age=604800, httponly=True); return r
    return RedirectResponse(url="/login?error=1", status_code=303)
@app.get("/logout")
async def logout(): r = RedirectResponse(url="/login", status_code=303); r.delete_cookie("session_id"); return r

@app.get("/live/{channel_name:path}")
async def proxy_live(request: Request, channel_name: str, url: str):
    verify_token(request) # 防盗链校验
    client_ip = request.headers.get("x-real-ip") or request.client.host
    return StreamingResponse(stream_pool.subscribe(channel_name, url, client_ip), media_type="video/mp2t", headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"})

@app.get("/epg.xml")
async def g_epg(request: Request):
    verify_token(request)
    return Response(content=state.epg_xml, media_type="application/xml")
@app.get("/epg.xml.gz")
async def g_epgz(request: Request):
    verify_token(request)
    return Response(content=state.epg_gz, media_type="application/gzip")

@app.post("/add_source")
async def a_s(request: Request, name: str = Form(...), url: str = Form(...), type: str = Form(...)):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session: session.add(Source(name=name, url=url, type=type)); session.commit()
    return RedirectResponse(url="/", status_code=303)
@app.get("/delete/{sid}")
async def delete_s(request: Request, sid: int):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        s = session.get(Source, sid); 
        if s: session.delete(s); session.commit()
    return RedirectResponse(url="/", status_code=303)
@app.post("/add_epg_source")
async def add_epg_src(request: Request, name: str = Form(...), url: str = Form(...)):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session: session.add(EPGSource(name=name, url=url)); session.commit(); asyncio.create_task(update_epg_task())
    return RedirectResponse(url="/", status_code=303)
@app.get("/delete_epg/{eid}")
async def delete_e(request: Request, eid: int):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session:
        e = session.get(EPGSource, eid);
        if e: session.delete(e); session.commit()
    return RedirectResponse(url="/", status_code=303)
@app.get("/refresh")
async def manual_refresh(request: Request):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    asyncio.create_task(update_epg_task()); return RedirectResponse(url="/", status_code=303)
