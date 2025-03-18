# kafka/lamport/vector management 
# we can add more file to implement logic of it

import asyncio
import websockets
import json
import aioredis
import aiomysql
from query_mysql import get_chatrooms_from_MySQL, get_chatroom_members_from_MySQL, get_chat_history_from_MySQL

# 记录已连接的用户（username -> websocket）
connected_users = {}

async def get_chatrooms(mysql_conn_pool, username):
    """
    从 MySQL 查询用户所属的聊天室
    """
    chatrooms = []
    # chatrooms = await redis.smembers(f"user:{username}:chatrooms")
    async with mysql_conn_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            # cursor.execute(f"SELECT chatroom.room_name from belonging, chatroom,  where belonging")
            chatrooms = await get_chatrooms_from_MySQL(cursor, username)
    return chatrooms

async def get_chatroom_members(mysql_conn_pool, chatroom):
    """
    从 MySQL 查询聊天室的所有成员
    """
    # members = await redis.smembers(f"chatroom:{chatroom}")
    members = {}
    async with mysql_conn_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            # cursor.execute(f"SELECT chatroom.room_name from belonging, chatroom,  where belonging")
            members = await get_chatroom_members_from_MySQL(cursor, chatroom)
    return members

async def get_chat_history(mysql_conn_pool, chatroom):
    """
    query chat history of the chatrooom from mysql
    """
    msg_history = {}
    async with mysql_conn_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await conn.commit()
            # cursor.execute(f"SELECT chatroom.room_name from belonging, chatroom,  where belonging")
            msg_history = await get_chat_history_from_MySQL(cursor, chatroom)
    return msg_history


async def register(websocket, redis, mysql_conn_pool):
    """
    handle user log in, connect to the user, get user's chatroom info from redis and chat history from mysql
    """
    try:
        # 接收客户端的登录信息（格式：{"username": "A"}）
        message = await websocket.recv()
        data = json.loads(message)

        username = data.get("username")
        if not username:
            await websocket.send(json.dumps({"error": "Invalid login data"}))
            return None, None
        elif username == "Producer":
            print("Producer connected")
            # await websocket.send(json.dumps({"success": "Producer connected to WebSocket Server 1"}))
        elif username == "Consumer":
            print("Consumer connected")
            # await websocket.send(json.dumps({"success":"Consumer connected to WebSocket Server 1"}))
        elif username in connected_users:
            await websocket.send(json.dumps({"error":"User has already logged in"}))
            return None, None

        # 获取用户所属的聊天室列表
        chatrooms = await get_chatrooms(mysql_conn_pool, username)
        # if not chatrooms:
        #     await websocket.send(json.dumps({"error": "User is not in any chatroom"}))
        #     return None, None
        
        # 记录用户到 websocket 的映射
        connected_users[username] = websocket
        await redis.sadd("connected_users", username)

        # print(f"User {username} connected, chatrooms: {chatrooms}")
        print(f"User {username} connected")

        # return username, chatrooms
        return username
    except Exception as e:
        print(f"Error during websocket connection: {e}")
        return None, None
    
async def request_handeler(websocket, path, redis, mysql_conn_pool):
    """
    handle clients' request (joining a channel / send messages in a chatroom)
    """
    username = await register(websocket, redis, mysql_conn_pool)
    try:
        async for req in websocket:
            print(f"Received request from {username}: {req}")

            try:
                data = json.loads(req)
                action = data.get("operation") # could be 'join', 'client message' or 'sorted message'
                if action == "join_chatroom":
                    await join_group_handler(websocket, redis, mysql_conn_pool, data, username)
                elif action == "client_message":
                    await chat_receiving_handler(websocket, redis, mysql_conn_pool, data, username)
                else:
                    await chat_sending_handler(websocket, redis, mysql_conn_pool, data, username)
                    
            except json.JSONDecodeError:
                print("Invalid JSON format received")
                await websocket.send(json.dumps({"error": "Invalid JSON format"}))
    
    except websockets.exceptions.ConnectionClosed:
        print(f"User {username} disconnected")
        del connected_users[username]
        await redis.srem("connected_user", username)
    
    finally:
        # 断开连接后清理数据
        if username in connected_users:
            del connected_users[username]
            await redis.srem("connected_user", username)
    
async def join_group_handler(websocket, redis, mysql_conn_pool, data, username):
    """
    handle clients' request of joining a channel (chatroom)
    """
    chatroom_name = data.get('chatroom')
    # chatrooms = await get_chatrooms(mysql_conn_pool, username)
    # if chatroom_name not in chatrooms:
    #    print(f"successfully join chatroom: {chatroom_name}")
        
    # await redis.sadd(f"user:{username}:chatrooms", chatroom_name)
    # await redis.sadd(f"chatroom:{chatroom_name}:users", username)
    print(f"{username} join chatroom: {chatroom_name}")
    """
    Get Chatroom History
    """
    chat_history = await get_chat_history(mysql_conn_pool, chatroom_name)
    for chat in chat_history:
        sender = chat["user"]
        msg = chat["message"]
        await websocket.send(json.dumps({"operation": "new_message", "chatroom": chatroom_name, "message": {"sender": sender, "content": msg}}))

    """
    write into mysql
    """
    """
    async with mysql_conn_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            
            insert op
            
            # cursor.execute(f"INSERT INTO belonging ()  {chatroom_name}")

    """
    
async def chat_receiving_handler(websocket, redis, mysql_conn_pool, data, username):
    """
    handle messages received from websocket connected user
    upload to cluster or send to producer to ensure the order
    """
    # username, chatrooms = await register(websocket, redis)
    # username = await register(websocket, redis)
    # if not username or not chatrooms:
    #     return

    sender = data.get("sender")
    chatroom_name = data.get("chatroom")
    content = data.get("content")

    # make sure the sender of the message matches the login information
    if sender != username:
        print(f"Warning: {username} tried to send message as {sender}")
        return
    
    # make sure users only speak in a joined chatroom
    chatrooms = await get_chatrooms(mysql_conn_pool, username)
    if chatroom_name not in chatrooms:
        print(f"Warning: {username} is not authorized to speak in {chatroom_name}")
        return
    # send this message to Kafka producer
    producer_conn = connected_users["Producer"]
    await producer_conn.send(json.dumps({"sender": username, "chatroom": chatroom_name,"content": content}))
    
async def chat_sending_handler(websocket, redis, mysql_conn_pool, data, username):
    """
    handle sorted messages from consumer and send them to corresponding users online
    """
    sender = data.get("sender")
    chatroom_name = data.get("chatroom")
    content = data.get("content")

    chatroom_members = await get_chatroom_members(mysql_conn_pool,chatroom_name)
    for user in chatroom_members:
        if user in connected_users:
            socket = connected_users[user]
            await socket.send(json.dumps({"operation": "new_message","chatroom": chatroom_name,"message": {"sender": sender, "content": content}}))

async def main():
    # connect to Redis
    redis = await aioredis.from_url("redis://54.167.123.207:6379")
    await redis.flushall()
    
    # connect to mysql
    master_pool = await aiomysql.create_pool(
        host="54.89.138.139", # MySQL 服务器1地址
        port = 3306,
        user="admin",
        password="Entishl-0606",
        db="chat_system",
        minsize=1,
        maxsize=20
    )
    '''
    slave_pool = aiomysql.create_pool(
        host="server2",
        user="root",
        password="password",
        db="testdb",
        minsize=1,
        maxsize=20
    )
    '''
    async def handler(websocket, path):
        await request_handeler(websocket, path, redis, master_pool)

    # 启动 WebSocket 服务器
    async with websockets.serve(handler, "0.0.0.0", 8000, ping_interval=20, ping_timeout=4000):
        print("Chat server started at ws://0.0.0.0:8000")
        await asyncio.Future()  # 服务器保持运行

if __name__ == "__main__":
    asyncio.run(main())