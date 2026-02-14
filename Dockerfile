FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 创建存放数据库的目录
RUN mkdir -p /app/data

EXPOSE 8000

# 运行
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
