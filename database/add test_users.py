import pymysql

# MySQL 数据库配置
DB_HOST = "52.207.240.16"  # 替换为你的 MySQL 服务器地址（例如 localhost 或 EC2 IP）
DB_USER = "admin"  # 替换为你的 MySQL 用户名
DB_PASSWORD = "Entishl-0606"  # 替换为你的 MySQL 密码
DB_NAME = "chat_system"  # 数据库名称
DB_PORT = 3306  # MySQL 端口，默认 3306

# 连接 MySQL 数据库
def get_connection():
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )
        return conn
    except pymysql.MySQLError as e:
        print(f"Fail to connect: {e}")
        return None

# **插入用户 & 加入聊天室**
def insert_users_and_join_chatroom():
    conn = get_connection()
    if not conn:
        print("无法连接数据库")
        return
    
    try:
        with conn.cursor() as cursor:
            # **确保聊天室 UCI-CS 存在**
            cursor.execute("INSERT IGNORE INTO chatrooms (room_name) VALUES ('UCI-CS');")
            conn.commit()

            # 获取 UCI-CS 聊天室的 room_id
            cursor.execute("SELECT room_id FROM chatrooms WHERE room_name = 'UCI-CS';")
            room_id = cursor.fetchone()["room_id"]

            # **批量插入 1000 个用户**
            users = [(str(i), f"password{i}") for i in range(1, 1001)]  # 用户名 & 密码
            cursor.executemany("INSERT IGNORE INTO users (user_name, password) VALUES (%s, %s);", users)
            conn.commit()

            # **获取用户 ID 并加入聊天室**
            cursor.execute("SELECT user_id, user_name FROM users;")
            user_map = {row["user_name"]: row["user_id"] for row in cursor.fetchall()}  # 映射用户名 -> ID

            belonging_data = [(user_map[str(i)], room_id) for i in range(1, 1001)]
            cursor.executemany("INSERT IGNORE INTO user_chatrooms (user_id, room_id) VALUES (%s, %s);", belonging_data)
            conn.commit()

            print("成功插入 1000 个用户，并加入 UCI-CS 聊天室！")

    except pymysql.MySQLError as e:
        print(f"数据库操作失败: {e}")
    finally:
        conn.close()

# **运行函数**
if __name__ == "__main__":
    insert_users_and_join_chatroom()