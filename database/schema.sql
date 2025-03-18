# MySQL 数据库表结构
-- 用户表
CREATE TABLE user (
    user_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_name VARCHAR(50) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL
);

-- 聊天室表
CREATE TABLE chatroom (
    room_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    room_name VARCHAR(100) UNIQUE NOT NULL
);

-- 用户-聊天室关系表
CREATE TABLE belonging (
    user_id BIGINT NOT NULL,
    room_id BIGINT NOT NULL,
    PRIMARY KEY (user_id, room_id),
    FOREIGN KEY (user_id) REFERENCES user(user_id) ON DELETE CASCADE,
    FOREIGN KEY (room_id) REFERENCES chatroom(room_id) ON DELETE CASCADE
);

-- 消息表
CREATE TABLE messages (
    message_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    room_id BIGINT NOT NULL,
    sender_id BIGINT NOT NULL,
    content TEXT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (room_id) REFERENCES chatroom(room_id) ON DELETE CASCADE,
    FOREIGN KEY (sender_id) REFERENCES user(user_id) ON DELETE CASCADE
);
