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
        # 别名映射与模板缓存
        self.alias_map = {}
        self.regex_aliases = []
        self.categories = [] # [{name: '央视', channels: []}]
        self.channel_to_category = {}
state = GlobalState()

# --- 2. 别名与分类解析引擎 ---

def load_alias_and_demo():
    """解析 alias.txt 和 demo.txt"""
    # 1. 加载别名
    alias_path = "alias.txt"
    state.alias_map = {}
    state.regex_aliases = []
    if os.path.exists(alias_path):
        with open(alias_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"): continue
                parts = line.split(",")
                main_name = parts[0].strip()
                for alias in parts[1:]:
                    alias = alias.strip()
                    if alias.startswith("re:"):
                        try:
                            pattern = re.compile(alias[3:])
                            state.regex_aliases.append((pattern, main_name))
                        except: pass
                    else:
                        state.alias_map[alias.upper()] = main_name

    # 2. 加载模板分类
    demo_path = "demo.txt"
    state.categories = []
    state.channel_to_category = {}
    if os.path.exists(demo_path):
        current_cat = None
        with open(demo_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                if ",#genre#" in line:
                    cat_name = line.split(",")[0].strip()
                    current_cat = {"name": cat_name, "channels": []}
                    state.categories.append(current_cat)
                elif current_cat is not None:
                    channel_name = line.strip()
                    current_cat["channels"].append(channel_name)
                    state.channel_to_category[channel_name] = current_cat["name"]

def get_standard_name(raw_name: str) -> str:
    """根据 alias.txt 将原始名称标准化"""
    name_up = raw_name.strip().upper()
    # 1. 精确匹配
    if name_up in state.alias_map:
        return state.alias_map[name_up]
    # 2. 正则匹配
    for pattern, main_name in state.regex_aliases:
        if pattern.search(raw_name):
            return main_name
    # 3. 兜底清洗 (旧逻辑)
    clean_name = raw_name.upper().replace(" ", "")
    patterns = [r"\(.*\)", r"（.*）", r"HD", r"高清", r"超清", r"4K", r"8K", r"蓝光", r"V\d", r"\(备用\)", r"\[.*\]", r"频道", r"-"]
    for p in patterns: clean_name = re.sub(p, "", clean_name)
    return clean_name.strip()

# --- 3. 缓冲池引擎 (保持稳定) ---

class StreamPool:
    def __init__(self): self.streams: Dict = {}
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
                        if r.status_code != 200: raise Exception()
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
                                except: pass
            except:
                if stream_id in self.streams: self.streams[stream_id]["in_speed"] = "重连中..."
                await asyncio.sleep(3)
        self.streams.pop(stream_id, None)

    async def subscribe(self, name: str, url: str, client_ip: str):
        stream_id = hashlib.md5(url.encode()).hexdigest()
        client_id = hashlib.md5(f"{client_ip}{time.time()}".encode()).hexdigest()[:8]
        if stream_id not in self.streams:
            self.streams[stream_id] = {"name": name, "url": url, "queues": [], "clients": {}, "info": {"res": "探测中...", "codec": "探测中..."}, "in_speed": "0 KB/s", "out_speed": "0 KB/s", "buffer_level": 0, "history": deque(maxlen=50)}
            asyncio.create_task(self._fetcher(stream_id, url, name))
            await asyncio.sleep(0.5)
        queue = asyncio.Queue(maxsize=200)
        if stream_id in self.streams:
            for old in list(self.streams[stream_id]["history"]): await queue.put(old)
            self.streams[stream_id]["queues"].append(queue)
            self.streams[stream_id]["clients"][client_id] = {"ip": client_ip, "out_bytes": 0, "speed": "0 KB/s", "last_ts": time.time()}
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
                self.streams[stream_id]["buffer_level"] = int((queue.qsize() / 200) * 100)
                yield chunk
        finally:
            if stream_id in self.streams:
                self.streams[stream_id]["queues"].remove(queue)
                self.streams[stream_id]["clients"].pop(client_id, None)

stream_pool = StreamPool()

# --- 4. EPG 聚合逻辑 ---

async def update_epg_task():
    if state.is_epg_updating: return
    state.is_epg_updating = True
    load_alias_and_demo() # 顺便重载模板
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

@app.on_event("startup")
async def startup():
    load_alias_and_demo()
    asyncio.create_task(update_epg_task())
    async def loop():
        while True: await asyncio.sleep(3600 * 6); await update_epg_task()
    asyncio.create_task(loop())

# --- 5. 路由与分发逻辑 ---

@app.get("/playlist.m3u")
@app.get("/playlist.txt")
async def get_playlist(request: Request, proxy: bool = False):
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", str(request.base_url.netloc))
    base_url = f"{scheme}://{host}"
    
    # 实时抓取并处理
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
        p_mode = int(session.get(Setting, "proxy_mode").value)
    
    unique_channels = [] # List of dicts
    seen_urls = set()
    
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        tasks = [client.get(s.url) for s in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, resp in enumerate(results):
            if isinstance(resp, Exception): continue
            lines = resp.text.split('\n')
            for j in range(len(lines)):
                line = lines[j].strip()
                name, u, group = None, None, ""
                if line.startswith("#EXTINF"):
                    raw_name = line.split(',')[-1].strip()
                    for k in range(j+1, min(j+5, len(lines))):
                        next_line = lines[k].strip()
                        if next_line.startswith("http"):
                            name, u = raw_name, next_line; break
                elif ',' in line and not line.startswith("#"):
                    parts = line.split(',', 1)
                    name, u = parts[0].strip(), parts[1].strip()
                
                if name and u and u not in seen_urls:
                    standard_name = get_standard_name(name)
                    cat_name = state.channel_to_category.get(standard_name, "其他频道")
                    use_proxy = (proxy) or (p_mode == 2) or (p_mode == 1 and ("/udp/" in u or "/rtp/" in u))
                    unique_channels.append({
                        "name": standard_name, "url": u, "group": cat_name, "use_proxy": use_proxy
                    })
                    seen_urls.add(u)

    # 排序：根据 demo.txt 中的分类顺序和频道顺序
    cat_order = {cat['name']: i for i, cat in enumerate(state.categories)}
    cat_order["其他频道"] = 999
    
    def get_sort_key(ch):
        # 1. 分类权重
        cat_weight = cat_order.get(ch['group'], 998)
        # 2. 频道在分类内的权重
        ch_weight = 999
        if ch['group'] != "其他频道":
            for i, name in enumerate(state.categories[cat_weight]['channels']):
                if name == ch['name']: ch_weight = i; break
        return (cat_weight, ch_weight, ch['name'])

    unique_channels.sort(key=get_sort_key)
    state.last_playlist_request = time.time()

    if request.url.path.endswith('.txt'):
        txt_lines, cur_cat = [], ""
        for c in unique_channels:
            if c['group'] != cur_cat:
                cur_cat = c['group']; txt_lines.append(f"{cur_cat},#genre#")
            p_url = f"{base_url}/live/{c['name']}?url={urllib.parse.quote(c['url'], safe='')}" if c['use_proxy'] else c['url']
            txt_lines.append(f"{c['name']},{p_url}")
        return Response(content="\n".join(txt_lines), media_type="text/plain")
    
    output = f'#EXTM3U x-tvg-url="{base_url}/epg.xml.gz"\n'
    for c in unique_channels:
        logo = f"{LOGO_BASE}{c['name'].upper()}.png"
        p_url = f"{base_url}/live/{c['name']}?url={urllib.parse.quote(c['url'], safe='')}" if c['use_proxy'] else c['url']
        output += f'#EXTINF:-1 tvg-logo="{logo}" group-title="{c["group"]}",{c["name"]}\n{p_url}\n'
    return Response(content=output, media_type="application/x-mpegurl")

# --- 其余管理路由保持不变 ---

@app.get("/api/status")
async def api_status():
    active = []
    t_in, t_out, t_peers = 0, 0, 0
    for s_id, d in stream_pool.streams.items():
        try: t_in += float(d["in_speed"].split(' ')[0]); t_out += float(d["out_speed"].split(' ')[0])
        except: pass
        t_peers += len(d["clients"])
        active.append({"name": d["name"], "url": d["url"], "in_speed": d["in_speed"], "out_speed": d["out_speed"], "peers": len(d["clients"]), "info": d["info"], "buffer": f"{d.get('buffer_level', 0)}/100", "clients": list(d["clients"].values())})
    return {"active_streams": active, "is_checking": state.is_epg_updating, "last_epg": time.strftime("%H:%M:%S", time.localtime(state.last_epg_update)) if state.last_epg_update else "待同步", "last_m3u": time.strftime("%H:%M:%S", time.localtime(state.last_playlist_request)) if state.last_playlist_request else "从无请求", "kpis": {"total_in": f"{t_in:.1f} KB/s", "total_out": f"{t_out:.1f} KB/s", "stream_count": len(active), "peer_count": t_peers}, "epg_logs": state.epg_logs}

@app.get("/")
async def index(request: Request):
    if not is_authenticated(request): return RedirectResponse(url="/login", status_code=303)
    with Session(engine) as session: return templates.TemplateResponse("index.html", {"request": request, "sources": session.exec(select(Source)).all(), "epg_sources": session.exec(select(EPGSource)).all(), "epg_days": session.get(Setting, "epg_days").value, "proxy_mode": session.get(Setting, "proxy_mode").value})
@app.get("/login", response_class=HTMLResponse)
async def l_p(request: Request): return templates.TemplateResponse("login.html", {"request": request})
@app.post("/login")
async def l_post(username: str = Form(...), password: str = Form(...)):
    if username == ADMIN_USER and password == ADMIN_PASS:
        r = RedirectResponse(url="/", status_code=303); r.set_cookie(key="session_id", value=SECRET_KEY, max_age=604800, httonly=True); return r
    return RedirectResponse(url="/login?error=1", status_code=303)
@app.get("/logout")
async def l_out(): r = RedirectResponse(url="/login", status_code=303); r.delete_cookie("session_id"); return r
@app.get("/live/{channel_name}")
async def proxy_live(request: Request, channel_name: str, url: str):
    client_ip = request.headers.get("x-real-ip") or request.client.host
    return StreamingResponse(stream_pool.subscribe(channel_name, url, client_ip), media_type="video/mp2t", headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"})
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
    with Session(engine) as session: session.add(EPGSource(name=name, url=url)); session.commit()
    asyncio.create_task(update_epg_task()); return RedirectResponse(url="/", status_code=303)
@app.get("/delete_epg/{eid}")
async def d_e(eid: int):
    with Session(engine) as session:
        e = session.get(EPGSource, eid);
        if e: session.delete(e); session.commit()
    return RedirectResponse(url="/", status_code=303)
@app.post("/update_global_settings")
async def u_g(proxy_mode: str = Form(...), epg_days: str = Form(...)):
    with Session(engine) as session:
        session.get(Setting, "proxy_mode").value = proxy_mode; session.get(Setting, "epg_days").value = epg_days; session.commit()
    return RedirectResponse(url="/", status_code=303)
@app.get("/refresh")
async def ref(): asyncio.create_task(update_epg_task()); return RedirectResponse(url="/", status_code=303)
