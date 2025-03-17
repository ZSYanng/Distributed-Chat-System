import asyncio
import websockets
import json
import random
import string
import mysql.connector
import aiohttp  # 用于异步 HTTP 请求

# MySQL 配置
DB_CONFIG = {
    "host": "52.207.240.16",  # MySQL 服务器 IP
    "port": 3306,              # MySQL 默认端口
    "user": "admin",       # 数据库用户名
    "password": "Entishl-0606",  # 数据库密码
    "database": "chat_system"      # 需要连接的数据库
}

# WebSocket API URL (用于获取 WebSocket 地址)
API_URL = "http://169.234.109.212:5000/get_ws"

# 聊天室
CHATROOMS = ["UCI-CS", "Technology", "Sports"]

# 用户总数
NUM_USERS = 1000

# 记录前 5 个用户的消息
RECORD_USERS = 5
recorded_messages = [[] for _ in range(RECORD_USERS)]


# 生成唯一用户名并插入数据库，并加入所有聊天室
def generate_unique_users():
    db = mysql.connector.connect(**DB_CONFIG)
    cursor = db.cursor()

    usernames = set()
    while len(usernames) < NUM_USERS:
        username = "user_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
        cursor.execute("SELECT COUNT(*) FROM users WHERE user_name = %s", (username,))
        if cursor.fetchone()[0] == 0:
            usernames.add(username)

    # 批量插入 users 表
    sql_users = "INSERT INTO users (user_name, password) VALUES (%s, %s)"
    values_users = [(username, "password123") for username in usernames]
    cursor.executemany(sql_users, values_users)

    # 获取所有 user_id
    cursor.execute("SELECT user_id, user_name FROM users")
    user_data = cursor.fetchall()  # [(user_id, "user_xxx"), ...]

    # 获取所有聊天室的 room_id
    cursor.execute("SELECT room_id, room_name FROM chatrooms")
    room_data = cursor.fetchall()  # [(room_id, "UCI-CS"), ...]

    room_map = {room_name: room_id for room_id, room_name in room_data}

    # 插入 user_chatrooms 表，每个用户加入所有聊天室
    sql_chatrooms = "INSERT INTO user_chatrooms (user_id, room_id) VALUES (%s, %s)"
    values_chatrooms = [
        (user_id, room_map[room_name])
        for user_id, _ in user_data
        for room_name in CHATROOMS
    ]
    cursor.executemany(sql_chatrooms, values_chatrooms)

    db.commit()
    cursor.close()
    db.close()

    return [user_name for _, user_name in user_data]  # 返回用户名列表


# 发送 HTTP 请求获取 WebSocket 地址
async def get_websocket_address(username):
    async with aiohttp.ClientSession() as session:
        async with session.post(API_URL, json={"username": username}) as response:
            data = await response.json()
            return data["ws_address"]  # 服务器返回的 WebSocket 地址


# WebSocket 客户端
async def websocket_client(user_id, username):
    ws_address = await get_websocket_address(username)  # 先获取 WebSocket 地址

    async with websockets.connect(ws_address) as websocket:
        # 发送用户名到 WebSocket
        await websocket.send(json.dumps({"username": username}))

        # 随机选择聊天室
        selected_chatroom = random.choice(CHATROOMS)

        # 发送加入聊天室消息
        join_message = json.dumps({
            "operation": "join_chatroom",
            "user": username,
            "chatroom": selected_chatroom
        })
        await websocket.send(join_message)

        # 发送消息
        async def send_messages():
            for i in range(10):  # 每个用户发送 10 条消息
                message_content = f"Message {i} from {username}"
                message = json.dumps({
                    "operation": "client_message",
                    "sender": username,
                    "chatroom": selected_chatroom,
                    "content": message_content
                })
                await websocket.send(message)
                await asyncio.sleep(random.uniform(0.1, 0.5))  # 随机等待时间，模拟真实发送

        # 记录 WebSocket 消息
        async def receive_messages():
            while True:
                try:
                    response = await websocket.recv()
                    data = json.loads(response)
                    if data.get("operation") == "new_message":
                        chatroom = data["chatroom"]
                        msg = data["message"]
                        sender = msg.get("sender", "Unknown")
                        content = msg["content"]

                        log_entry = f"[{ws_address}] {chatroom} - {sender}: {content}"

                        # 记录前 5 个用户的消息
                        if user_id < RECORD_USERS:
                            recorded_messages[user_id].append(log_entry)
                except websockets.exceptions.ConnectionClosed:
                    break

        # 并发执行
        await asyncio.gather(send_messages(), receive_messages())


# 启动 1000 个 WebSocket 连接
async def main():
    usernames = generate_unique_users()  # 先插入 1000 个唯一用户，并加入聊天室
    tasks = [websocket_client(i, usernames[i]) for i in range(NUM_USERS)]
    await asyncio.gather(*tasks)

    # 保存 5 个用户的 WebSocket 消息
    for i in range(RECORD_USERS):
        with open(f"websocket_log_user_{i}.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(recorded_messages[i]))


# 运行 WebSocket 测试
if __name__ == "__main__":
    asyncio.run(main())
