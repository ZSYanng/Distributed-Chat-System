import asyncio
import websockets
import json
import random
import string
import aiohttp
import mysql.connector
import concurrent.futures

# MySQL 配置
DB_CONFIG = {
    "host": "54.89.138.139",  # MySQL 服务器 IP
    "port": 3306,  # MySQL 默认端口
    "user": "admin",  # 数据库用户名
    "password": "Entishl-0606",  # 数据库密码
    "database": "chat_system"  # 需要连接的数据库
}

# WebSocket API URL (used to get WebSocket address)
API_URL = "http://169.234.68.2:5000/login"

# Chatrooms
CHATROOMS = ["UCI-CS", "Technology", "Sports"]

# Number of users
NUM_USERS = 100  # 1000

# Number of users to log messages
RECORD_USERS = 6
recorded_messages = [[] for _ in range(RECORD_USERS)]


def clear_messages():
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor()
        cursor.execute("DELETE FROM messages;")
        db.commit()

        print("[INFO] Successfully deleted all records from messages table.")

    except mysql.connector.Error as err:
        print(f"[ERROR] MySQL error: {err}")


# Fetch WebSocket server address for a user
async def get_websocket_address(username):
    async with aiohttp.ClientSession() as session:
        async with session.post(API_URL, json={"username": username, "password": "123"}) as response:
            data = await response.json()
            print(f"[INFO] Received WebSocket address for {username}: {data['websocket_server']}")
            return data["websocket_server"]  # Server returns WebSocket URL


# WebSocket Client
async def websocket_client(user_id, username):
    ws_address = await get_websocket_address(username)  # Fetch WebSocket address first

    async with websockets.connect(ws_address['url']) as websocket:
        print(f"[INFO] {username} connected to WebSocket server at {ws_address}")

        # Send username to WebSocket
        await websocket.send(json.dumps({"username": username}))
        print(f"[INFO] {username} sent username to WebSocket server.")

        # Randomly select a chatroom
        selected_chatroom = "UCI-CS"

        # Send join chatroom message
        join_message = json.dumps({
            "operation": "join_chatroom",
            "user": username,
            "chatroom": selected_chatroom
        })
        await websocket.send(join_message)
        print(f"[INFO] {username} joined chatroom: {selected_chatroom}")

        # Send messages
        async def send_messages():
            for i in range(3):
                message_content = f"Message {i} from {username}"
                message = json.dumps({
                    "operation": "client_message",
                    "sender": username,
                    "chatroom": selected_chatroom,
                    "content": message_content
                })
                await websocket.send(message)
                print(f"[SEND] {username} -> {selected_chatroom}: {message_content}")
                await asyncio.sleep(random.uniform(0.1, 0.5))  # Simulate random sending delay

        # Receive WebSocket messages and log them
        async def receive_messages():
            received_count = 0
            expected_messages = 3 * NUM_USERS
            while received_count < expected_messages:
                try:
                    response = await websocket.recv()
                    data = json.loads(response)
                    if data.get("operation") == "new_message":
                        chatroom = data["chatroom"]
                        msg = data["message"]
                        sender = msg.get("sender", "Unknown")
                        content = msg["content"]

                        log_entry = f"[{ws_address}] {chatroom} - {sender}: {content}"
                        print(f"[RECEIVED] {chatroom} <- {sender}: {content}")

                        received_count += 1
                        # Record messages for the first 5 users
                        if user_id < RECORD_USERS:
                            recorded_messages[user_id].append(log_entry)
                except websockets.exceptions.ConnectionClosed:
                    print(f"[ERROR] Connection closed for {username}")
                    break

        # Run both send and receive concurrently
        await asyncio.gather(send_messages(), receive_messages())


# Launch 1000 WebSocket connections using multithreading for login
async def run_websocket_clients():
    print("[INFO] Launching WebSocket clients using multithreading...")
    usernames = [str(i) for i in range(1, NUM_USERS + 1)]  # '1' to '1000'

    # with concurrent.futures.ThreadPoolExecutor(max_workers=1000) as executor:
    #     futures = [executor.submit(asyncio.run, websocket_client(i, usernames[i])) for i in range(NUM_USERS)]
    #     concurrent.futures.wait(futures)  # Wait for all threads to finish
    tasks = [asyncio.create_task(websocket_client(i, usernames[i - 1])) for i in range(1, NUM_USERS + 1)]

    # **等待所有任务完成**
    await asyncio.gather(*tasks)

    print("[INFO] WebSocket test completed successfully.")

    # Save messages from the first 5 users
    for i in range(RECORD_USERS):
        if i != 0:
            filename = f"websocket_log_user_{i}.txt"
            with open(filename, "w", encoding="utf-8") as f:
                f.write("\n".join(recorded_messages[i]))
            print(f"[INFO] Messages recorded for user {i} saved to {filename}")


# Run WebSocket test
if __name__ == "__main__":
    clear_messages()
    asyncio.run(run_websocket_clients())
