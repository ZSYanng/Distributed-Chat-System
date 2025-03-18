import json
import threading
import websocket
from confluent_kafka import Producer

# Kafka 配置
KAFKA_BROKER = "192.168.21.105:9094"  # 连接 Kafka 集群
TOPIC = "chat-messages"  # 统一的聊天 Topic

conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'chat-producer',
    'enable.idempotence': True,
    'retries': 5
}

producer = Producer(conf)


# 发送消息到 Kafka
def send_to_kafka(group_id, user, message):
    try:
        partition = hash(group_id) % 3  # 直接使用 group_id 作为 partition
        key = user.encode("utf-8")
        value = json.dumps({"group": group_id, "user": user, "message": message}).encode("utf-8")

        producer.produce(TOPIC, key=key, value=value, partition=partition, callback=delivery_report)
        producer.flush()
    except Exception as e:
        print(f"❌ Failed to send message to Kafka: {e}")


# 回调函数
def delivery_report(err, msg):
    if err:
        print(f"Message sent fail: {err}")
    else:
        print(f"Message sent: {msg.value().decode()} to Partition {msg.partition()}")


# WebSocket 服务器地址（假设外部服务器的 WebSocket）
WEBSOCKET_SERVER = ["ws://169.234.109.212:8001", "ws://169.234.109.212:8002",
                    "ws://169.234.109.212:8003"]  # "ws://169.234.109.212:8001",


# 处理 WebSocket 消息
def on_message(ws, message):
    try:
        data = json.loads(message)
        group_id = str(data["chatroom"])  # 确保是字符串
        user = data["sender"]
        text = data["content"]
        print(f"Receive message from WebSocket: {data}")
        send_to_kafka(group_id, user, text)  # 发送到 Kafka
    except Exception as e:
        print(f"❌ Failed to process WebSocket message: {e}")


def on_open(ws):
    # **发送身份认证信息**
    auth_payload = json.dumps({"username": "Producer"})
    ws.send(auth_payload)
    print(f"🔐 Sent authentication : {auth_payload}")


# WebSocket 连接
def websocket_listener(server_url):
    ws = websocket.WebSocket()
    ws.connect(server_url)
    ws_app = websocket.WebSocketApp(server_url, on_open=on_open, on_message=on_message)
    ws_app.run_forever()  # ping_interval=30


# 启动 WebSocket 监听线程
for server in WEBSOCKET_SERVER:
    thread = threading.Thread(target=websocket_listener, args=(server,), daemon=True)
    thread.start()

print("🚀 WebSocket Producer start!")
while True:
    pass  # 保持运行


