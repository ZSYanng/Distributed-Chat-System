import { createClient } from 'redis';

// Redis 连接信息
const REDIS_HOST = 'localhost';
const REDIS_PORT = 6379;

// 创建 Redis 客户端
const publisher = createClient({ socket: { host: REDIS_HOST, port: REDIS_PORT } });
const subscriber = createClient({ socket: { host: REDIS_HOST, port: REDIS_PORT } });

// 连接 Redis
async function connectRedis() {
    try {
        await publisher.connect();
        await subscriber.connect();
        console.log("Connected to Redis Pub/Sub system.");
    } catch (error) {
        console.error("Redis connection failed:", error);
    }
}

// 发布消息到 Redis 频道
async function publishMessage(channel, message) {
    try {
        console.log(`Publishing message to ${channel}: ${message}`);
        await publisher.publish(channel, message);
    } catch (error) {
        console.error("Error in publishing:", error);
    }
}

// 订阅指定频道并监听消息
async function subscribeToChannel(channel) {
    try {
        console.log(`Subscribing to ${channel}...`);
        await subscriber.subscribe(channel, (message) => {
            console.log(`Received message from ${channel}: ${message}`);
        });
    } catch (error) {
        console.error("Error in subscribing:", error);
    }
}

// 初始化 Pub/Sub 逻辑
async function startPubSub() {
    await connectRedis();  // 连接 Redis

    // 订阅 "chatroom" 频道
    await subscribeToChannel("chatroom");

    // 5秒后测试发布一条消息
    setTimeout(async () => {
        await publishMessage("chatroom", "Hello, WebSocket Servers!");
    }, 5000);
}

// 启动 Pub/Sub 监听
startPubSub().catch(console.error);

