import asyncio
import aiomysql
import aioredis
import hashlib
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

# WebSocket 服务器信息
WEBSOCKET_SERVERS = {
    "server1": {"ip": "192.168.1.10", "port": 8001, "redis": "redis://192.168.1.10:6379"},
    "server2": {"ip": "192.168.1.11", "port": 8002, "redis": "redis://192.168.1.11:6379"},
    "server3": {"ip": "192.168.1.12", "port": 8003, "redis": "redis://192.168.1.12:6379"},
}

class UserRequest(BaseModel):
    username: str
    password: str

async def get_websocket_server():
    """ 查询 WebSocket 服务器的 Redis，获取连接数最少的服务器 """
    min_server = None
    min_connections = float("inf")

    for server_name, server_info in WEBSOCKET_SERVERS.items():
        try:
            redis = await aioredis.from_url(server_info["redis"])
            connections = await redis.scard("connected_users")
            connections = int(connections) if connections else 0
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
    """ 用户注册，并存入 MySQL """
    username = user.username
    password = user.password

    async with mysql_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT COUNT(*) FROM users WHERE username = %s", (username,))
            result = await cursor.fetchone()
            if result[0] > 0:
                raise HTTPException(status_code=400, detail="Username already exists")

            await cursor.execute("INSERT INTO users (username, password) VALUES (%s, %s)", (username, password))
            await conn.commit()

    server = await get_websocket_server()
    return {"message": "Registration successful", "websocket_server": server}

@app.post("/login")
async def login_user(user: UserRequest):
    """ 用户登录，并验证 MySQL 账号密码 """
    username = user.username
    password = user.password # 加密密码

    async with mysql_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT password FROM users WHERE username = %s", (username,))
            result = await cursor.fetchone()

            if not result or result[0] != password:
                raise HTTPException(status_code=400, detail="Invalid username or password")

    server = await get_websocket_server()
    return {"message": "Login successful", "websocket_server": server}

@app.on_event("startup")
async def startup():
    global mysql_pool
    mysql_pool = await aiomysql.create_pool(
        host="192.168.1.10",
        user="root",
        password="password",
        db="testdb",
        minsize=1,
        maxsize=20
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
