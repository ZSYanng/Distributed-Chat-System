import asyncio
import aioredis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

# WebSocket 服务器信息（IP & 端口 & 本地 Redis 地址）
WEBSOCKET_SERVERS = {
    "server1": {"ip": "192.168.1.10", "port": 8001, "redis": "redis://192.168.1.10:6379"},
    "server2": {"ip": "192.168.1.11", "port": 8002, "redis": "redis://192.168.1.11:6379"},
    "server3": {"ip": "192.168.1.12", "port": 8003, "redis": "redis://192.168.1.12:6379"},
}

class UserRequest(BaseModel):
    username: str
    password: str  # 实际项目应使用哈希加密，这里简化处理

async def get_websocket_server():
    """ 查询每个 WebSocket 服务器的本地 Redis，获取连接数最少的服务器 """
    min_server = None
    min_connections = float("inf")

    for server_name, server_info in WEBSOCKET_SERVERS.items():
        try:
            redis = await aioredis.from_url(server_info["redis"])
            connections = await redis.get("websocket:connections")
            connections = int(connections) if connections else 0
            print(f"{server_name}: {connections} connections")
            await redis.close()

            if connections < min_connections:
                min_connections = connections
                min_server = server_name
        except Exception as e:
            print(f"Failed to connect to Redis on {server_name}: {e}")

    if min_server:
        return WEBSOCKET_SERVERS[min_server]
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
