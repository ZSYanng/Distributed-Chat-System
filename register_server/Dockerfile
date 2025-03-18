# 使用 Python 3.9
FROM python:3.9

# 设置工作目录
WORKDIR /app

# 复制依赖文件并安装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目代码
COPY . .

# 运行 FastAPI 服务器
CMD ["uvicorn", "register_server:app", "--host", "0.0.0.0", "--port", "5000"]

