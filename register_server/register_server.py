import time
import asyncio
import aioredis
import aiomysql
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有域名（可修改为指定域）
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有请求方法（包括 OPTIONS）
    allow_headers=["*"],  # 允许所有请求头
)

host = "128.195.95.33"

connection_cnt = {}
update_time = {}
lock = asyncio.Lock()

# WebSocket 服务器信息（IP & 端口 & 本地 Redis 地址）
WEBSOCKET_SERVERS = {
    "server1": {"url": f"ws://{host}:8001", "redis": "redis://54.167.123.207:6379"},
    "server2": {"url": f"ws://{host}:8002", "redis": "redis://52.55.16.91:6379"},
    "server3": {"url": f"ws://{host}:8003", "redis": "redis://54.89.138.139:6379"}
}

class UserRequest(BaseModel):
    username: str
    password: str  # 实际项目应使用哈希加密，这里简化处理

async def get_websocket_server():
    """ 查询每个 WebSocket 服务器的本地 Redis，获取连接数最少的服务器 """
    global connection_cnt
    global update_time
    async with lock:
        min_server = None
        min_connections = float("inf")

        for server_name, server_info in WEBSOCKET_SERVERS.items():
            if server_name not in update_time or time.time() - update_time[server_name] > 1000:
                try:
                    # redis = await aioredis.from_url("redis://35.173.122.222:6379")
                    redis = await aioredis.from_url(server_info["redis"])
                    connections = await redis.scard("connected_users")
                    connections = int(connections) if connections else 0
                    connection_cnt[server_name] = connections
                    # print(f"{server_name}: {connections} connections")
                    await redis.close()
                    """
                    if connections < min_connections:
                        min_connections = connections
                        min_server = server_name
                    """
                    update_time[server_name] = time.time()
                
                except Exception as e:
                    print(f"Failed to connect to Redis on {server_name}: {e}")
            
            print(f"{server_name}: {connection_cnt[server_name]} connections")
            if connection_cnt[server_name] < min_connections:
                    min_connections = connection_cnt[server_name]
                    min_server = server_name


        if min_server:
            connection_cnt[min_server] += 1
            return WEBSOCKET_SERVERS[min_server]
            # return WEBSOCKET_SERVERS[min_server]
        else:
            raise HTTPException(status_code=500, detail="No available WebSocket servers")

@app.post("/register")
async def register_user(user: UserRequest):
    """ 处理用户注册请求，分配 WebSocket 服务器 """
    server = await get_websocket_server()
    return {"message": "Registration successful", "websocket_server": server}

@app.post("/login")
async def login_user(user: UserRequest):
    """ 处理用户登录请求，分配 WebSocket 服务器 """
    server = await get_websocket_server()
    return {"message": "Login successful", "websocket_server": server}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
