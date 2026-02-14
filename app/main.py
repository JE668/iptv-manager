from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlmodel import SQLModel, Field, Session, select, create_engine
import httpx
import re
import os

# --- 1. 数据库设置 ---
DB_FILE = "/app/data/iptv.db"
# 确保目录存在
os.makedirs("/app/data", exist_ok=True)
engine = create_engine(f"sqlite:///{DB_FILE}")

class Source(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    url: str
    type: str # m3u 或 txt

# 初始化数据库 (修正点：SQLModel 内部会通过 metadata 处理，不需要单独 import create_all)
SQLModel.metadata.create_all(engine)

app = FastAPI()

# 确保 templates 目录路径正确
templates = Jinja2Templates(directory="templates")

# --- 2. 后端管理界面 ---

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()
    return templates.TemplateResponse("index.html", {"request": request, "sources": sources})

@app.post("/add_source")
async def add_source(name: str = Form(...), url: str = Form(...), type: str = Form(...)):
    with Session(engine) as session:
        new_source = Source(name=name, url=url, type=type)
        session.add(new_source)
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

# --- 3. M3U 生成逻辑 ---

@app.get("/playlist.m3u")
async def get_m3u():
    channels = {}
    # tvg-logo 链接前缀
    logo_base = "https://gcore.jsdelivr.net/gh/taksssss/tv/icon/"
    
    with Session(engine) as session:
        sources = session.exec(select(Source)).all()

    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        for source in sources:
            try:
                resp = await client.get(source.url)
                content = resp.text
                if source.type == 'm3u':
                    lines = content.split('\n')
                    for i in range(len(lines)):
                        if "#EXTINF" in lines[i]:
                            # 提取频道名
                            name_match = re.search(r',([^,]+)$', lines[i])
                            if name_match:
                                name = name_match.group(1).strip()
                                # 下一行通常是 URL
                                if i + 1 < len(lines):
                                    url = lines[i+1].strip()
                                    if url.startswith("http"):
                                        if name not in channels: 
                                            channels[name] = url
                else:
                    # TXT 格式解析
                    for line in content.split('\n'):
                        if ',' in line:
                            parts = line.split(',', 1)
                            if len(parts) == 2:
                                name, url = parts
                                name = name.strip()
                                if name not in channels: 
                                    channels[name] = url.strip()
            except Exception as e:
                print(f"Error parsing source {source.name}: {e}")
                continue

    # 聚合 EPG (暂时硬编码，后续可改为配置)
    output = '#EXTM3U x-tvg-url="https://epg.170909.xyz:1799/t.xml.gz"\n'
    for name, url in channels.items():
        logo = f"{logo_base}{name}.png"
        output += f'#EXTINF:-1 tvg-name="{name}" tvg-logo="{logo}" group-title="聚合频道",{name}\n{url}\n'
    
    return Response(content=output, media_type="application/x-mpegurl")
