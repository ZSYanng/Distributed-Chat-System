import json
import mysql.connector
import websocket
from confluent_kafka import Consumer
import time
import threading

# Kafka configuration
KAFKA_BROKER = "192.168.21.105:9094"
TOPIC = "chat-messages"
GROUP_ID = "chat-consumer-group"

conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'enable.auto.commit': False,
    'auto.offset.reset': 'latest'  # 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])

# Database connection
DB_CONFIG = {
    "host": "52.207.240.16",  # MySQL 服务器 IP
    "port": 3306,  # MySQL 默认端口
    "user": "admin",  # 数据库用户名
    "password": "Entishl-0606",  # 数据库密码
    "database": "chat_system"  # 需要连接的数据库
}

# 连接 MySQL 数据库
try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("✅ Successfully connected to MySQL database!")
except mysql.connector.Error as e:
    print(f"❌ Failed to connect to MySQL: {e}")

# WebSocket kafka (push messages to online users)
WEBSOCKET_SERVERS = ["ws://169.234.109.212:8001", "ws://169.234.109.212:8002", "ws://169.234.109.212:8003"]  #
# 维护 WebSocket 连接
ws_clients = {}


# # 获取 WebSocket 连接，并发送身份认证信息
# def get_ws_connection(group_id):
#     if group_id not in ws_clients:
#         ws = websocket.WebSocket()
#         server_url = WEBSOCKET_SERVERS.get(group_id, WEBSOCKET_SERVERS["group1"])
#         ws.connect(server_url)

#         # **发送身份认证信息**
#         auth_payload = json.dumps({"username": "Consumer"})
#         ws.send(auth_payload)
#         print(f"🔐 Sent authentication to {server_url}: {auth_payload}")

#         ws_clients[group_id] = ws
#     return ws_clients[group_id]
def get_ws_connections():
    for server_url in WEBSOCKET_SERVERS:
        if server_url not in ws_clients:  # 避免重复连接
            ws = websocket.WebSocket()
            try:
                ws.connect(server_url)

                # **发送身份认证信息**
                auth_payload = json.dumps({"username": "Consumer"})
                ws.send(auth_payload)
                print(f"🔐 Sent authentication to {server_url}: {auth_payload}")

                # # 启动心跳线程
                # heartbeat_thread = threading.Thread(target=send_heartbeat, args=(ws, server_url), daemon=True)
                # heartbeat_thread.start()

                # 存储连接
                ws_clients[server_url] = ws
            except Exception as e:
                print(f"❌ Failed to connect to {server_url}: {e}")

    return ws_clients


def send_heartbeat(ws, server_url):
    while True:
        try:
            if ws.sock and ws.sock.connected:
                ws.send(json.dumps({"type": "ping"}))
                print(f"Sent heartbeat to {server_url}")
            time.sleep(30)
        except Exception as e:
            print(f"❌ Heartbeat failed for {server_url}: {e}")
            break  # 如果 WebSocket 断开，终止心跳线程


# Store messages in the database
def save_message(group, user, message, offset):
    sql1 = "SELECT room_id FROM chatrooms WHERE room_name = %s"
    cur.execute(sql1, (group,))
    room_id = cur.fetchone()[0]
    sql2 = "SELECT user_id FROM users WHERE user_name = %s"
    cur.execute(sql2, (user,))
    user_id = cur.fetchone()[0]
    sql3 = "INSERT INTO messages (`room_id`, `user_id`, `content`, `timestamp`) VALUES (%s, %s, %s, %s)"
    cur.execute(sql3, (room_id, user_id, message, offset))
    conn.commit()
    print(f"✅ Stored message: [{group}] {user}: {message} (Offset: {offset})")


# Process Kafka messages
def process_message(msg):
    data = json.loads(msg.value().decode("utf-8"))
    group_id = data["group"]
    user = data["user"]
    message = data["message"]
    offset = msg.offset()

    print(f"Received Kafka message: {data} (Offset: {offset})")

    # Store in the database
    save_message(group_id, user, message, offset)

    back_data = json.dumps({
        "operation": "sorted message",
        "sender": user,
        "chatroom": group_id,
        "content": message
    }).encode("utf-8")

    # Send to online users (simulate real-time notification)
    ws = get_ws_connections()
    for key, value in ws.items():
        value.send(back_data)
    print(f"🚀 Message sent to WebSocket Server")


print("Consumer is listening for Kafka messages...")
ws = get_ws_connections()
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        if process_message(msg):
            consumer.commit()  # ✅ 只有消息成功处理后才提交 Offset

except KeyboardInterrupt:
    print("Exiting Consumer")
finally:
    consumer.close()
    conn.close()
    for ws in ws_clients.values():
        ws.close()
