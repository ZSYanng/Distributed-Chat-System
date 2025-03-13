# kafka/lamport/vector management 
# we can add more file to implement logic of it

import asyncio
import websockets
import json
import aioredis
import aiomysql

# 记录已连接的用户（username -> websocket）
connected_users = {}

async def get_chatrooms_from_redis(redis, username):
    """
    从 Redis 查询用户所属的聊天室
    """
    chatrooms = await redis.smembers(f"user:{username}:chatrooms")
    return chatrooms

async def get_chatroom_members_from_redis(redis, chatroom):
    """
    从 Redis 查询聊天室的所有成员
    """
    members = await redis.smembers(f"chatroom:{chatroom}")
    return members

async def get_chat_history_from_mysql(mysql_conn, chatroom) {
    """
    query chat history of the chatrooom from mysql
    """
    
}

async def register(websocket, redis):
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

        # 获取用户所属的聊天室列表
        chatrooms = await get_chatrooms_from_redis(redis, username)
        # if not chatrooms:
        #     await websocket.send(json.dumps({"error": "User is not in any chatroom"}))
        #     return None, None
        
        # 记录用户到 websocket 的映射
        connected_users[username] = websocket

        # print(f"User {username} connected, chatrooms: {chatrooms}")
        print(f"User {username} connected")

        # return username, chatrooms
        return username
    except Exception as e:
        print(f"Error during registration: {e}")
        return None, None
    
async def request_handeler(websocket, path, redis, mysql_conn_pool):
    """
    handle clients' request (joining a channel / send messages in a chatroom)
    """
    username = await register(websocket, redis)
    mysql_conn = mysql_conn_pool[hash(username) % 3] # use hash code to assign users to different servers
    try:
        async for req in websocket:
            print(f"Received request from {username}: {req}")

            try:
                data = json.loads(req)
                action = data.get("action") # could be 'join', 'client message' or 'sorted message'
                if action == "join":
                    await join_group_handler(websocket, redis, mysql_conn, data, username)
                elif action == "client message":
                    await chat_receiving_handler(websocket, redis, mysql_conn, data, username)
                else:
                    await chat_sending_handler(websocket, redis, mysql_conn, data, username)
                    
            except json.JSONDecodeError:
                print("Invalid JSON format received")
                await websocket.send(json.dumps({"error": "Invalid JSON format"}))
    
    except websockets.exceptions.ConnectionClosed:
        print(f"User {username} disconnected")
    
    finally:
        # 断开连接后清理数据
        if username in connected_users:
            del connected_users[username]
    
async def join_group_handler(websocket, redis, mysql_conn, data, username):
    """
    handle clients' request of joining a channel (chatroom)
    """
    chatroom_name = data.get('chatroom')
    chatrooms = await get_chatrooms_from_redis(redis, username)
    if chatroom_name not in chatrooms:
        print()
        
    await redis.sadd(f"user:{username}:chatrooms", chatroom_name)
    await redis.sadd(f"chatroom:{chatroom_name}:users", username)

async def chat_receiving_handler(websocket, redis, mysql_conn, data, username):
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
    chatrooms = await get_chatrooms_from_redis(redis, username)
    if chatroom_name not in chatrooms:
        print(f"Warning: {username} is not authorized to speak in {chatroom_name}")
        return
    # send this message to cluster as producer
    
async def chat_sending_handler(websocket, redis, data, username):
    """
    handle sorted messages from consumer and send them to corresponding users online
    """