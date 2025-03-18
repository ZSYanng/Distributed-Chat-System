# Redis connection (cooperate with MySQL)
import redis
import pymysql

# Connect to Redis
r = redis.StrictRedis(host='35.173.122.222', port=6379, decode_responses=True)

# MySQL Setup
EC2_HOST = "35.173.122.222"  
DB_USER = "admin"  
DB_PASSWORD = "Entishl-0606"  
DB_NAME = "chat_system"  
DB_PORT = 3306  

# Get MySQL connection
def get_connection():
    try:
        connection = pymysql.connect(
            host=EC2_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,  
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Success!")
        return connection
    except pymysql.MySQLError as e:
        print(f"failure: {e}")
        return None

# Users sign up
def register_user(user_name, password):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO user (user_name, password) VALUES (%s, %s)", (user_name, password))
            connection.commit()
            # Redis update users information
            r.hset(f"user:{user_name}", "password", password)
            print(f"User {user_name} successfully registered")
    except pymysql.MySQLError as e:
        print(f"Failure: {e}")
    finally:
        connection.close()

# Users log in
def login_user(user_name, password):
    # Search from Redis
    stored_password = r.hget(f"user:{user_name}", "password")
    if stored_password:
        if stored_password == password:
            print(f"{user_name} logs in")
            return True
        else:
            print(f" {user_name} error")
            return False

    # Search from MySQL
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT password FROM user WHERE user_name = %s", (user_name,))
            result = cursor.fetchone()
            if result and result["password"] == password:
                # Successfully log in，write into Redis cache
                r.hset(f"user:{user_name}", "password", password)
                print(f"{user_name} logs in, and cache into Redis")
                return True
            else:
                print(f"Failure")
                return False
    finally:
        connection.close()

# Join in new chatroom
def join_chatroom(user_name, room_name):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            # Get User ID and Chatroom ID
            cursor.execute("SELECT user_id FROM user WHERE user_name = %s", (user_name,))
            user = cursor.fetchone()
            cursor.execute("SELECT room_id FROM chatroom WHERE room_name = %s", (room_name,))
            room = cursor.fetchone()
            if not user or not room:
                print(f"User or chatroom doesn't exist")
                return

            user_id, room_id = user["user_id"], room["room_id"]

            # insert MySQL
            cursor.execute("INSERT INTO belonging (user_id, room_id) VALUES (%s, %s)", (user_id, room_id))
            connection.commit()

            # Redis records
            r.sadd(f"belonging:{user_name}", room_name)
            r.sadd(f"chatroom:{room_name}", user_name)
            print(f"{user_name} joins in {room_name}")
    except pymysql.MySQLError as e:
        print(f"Failure: {e}")
    finally:
        connection.close()

# Exit chatroom
def leave_chatroom(user_name, room_name):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            # Get User ID and Chatroom ID
            cursor.execute("SELECT user_id FROM user WHERE user_name = %s", (user_name,))
            user = cursor.fetchone()
            cursor.execute("SELECT room_id FROM chatroom WHERE room_name = %s", (room_name,))
            room = cursor.fetchone()
            if not user or not room:
                print(f"User or chatroom doesn't exist")
                return

            user_id, room_id = user["user_id"], room["room_id"]

            # Delete MySQL recordings
            cursor.execute("DELETE FROM belonging WHERE user_id = %s AND room_id = %s", (user_id, room_id))
            connection.commit()

            # Redis deletes relationship
            r.srem(f"belonging:{user_name}", room_name)
            r.srem(f"chatroom:{room_name}", user_name)
            print(f"{user_name} exits chatroom {room_name}")
    finally:
        connection.close()

# Cache messages
def store_message(room_name, sender_name, content, max_messages=50):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            # Get User ID and room ID
            cursor.execute("SELECT room_id FROM chatroom WHERE room_name = %s", (room_name,))
            room = cursor.fetchone()
            cursor.execute("SELECT user_id FROM user WHERE user_name = %s", (sender_name,))
            user = cursor.fetchone()
            if not room or not user:
                print(f"User or chatroom doesn't exist")
                return

            room_id, sender_id = room["room_id"], user["user_id"]

            # Store in MySQL（permanent）
            cursor.execute("INSERT INTO messages (room_id, sender_id, content) VALUES (%s, %s, %s)",
                           (room_id, sender_id, content))
            connection.commit()

            # Cache in Redis
            message = f"{sender_name}: {content}"
            key = f"messages:{room_name}"
            r.lpush(key, message)
            r.ltrim(key, 0, max_messages - 1)

            r.publish("chat_messages", message)
            print(f"{sender_name} sends messages to {room_name}: {content}")
    finally:
        connection.close()

# Get the most recent messages
def get_recent_messages(room_name, num_messages=30):
    key = f"messages:{room_name}"
    messages = r.lrange(key, 0, num_messages - 1)

    if not messages:  # Redis has no data, search from MySQL
        connection = get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                SELECT user.user_name, messages.content FROM messages
                JOIN user ON messages.sender_id = user.user_id
                JOIN chatroom ON messages.room_id = chatroom.room_id
                WHERE chatroom.room_name = %s
                ORDER BY messages.timestamp DESC LIMIT %s
                """, (room_name, num_messages))
                messages = [f"{row['user_name']}: {row['content']}" for row in cursor.fetchall()]

                # Load to Redis
                for message in reversed(messages):
                    r.lpush(key, message)
                print(f"Load history messages of {room_name} from MySQL")
        finally:
            connection.close()
    
    return messages

# Clear Redis (If it is full)
def cleanup_redis(max_rooms=100):
    all_rooms = r.keys("messages:*")
    if len(all_rooms) > max_rooms:
        oldest_room = sorted(all_rooms, key=lambda k: r.object("idletime", k))[-1]
        r.delete(oldest_room)
        print(f"Delete the old chatroom {oldest_room}")

# Users offline
def user_logout(user_name):
    print(f"{user_name} exits the system")

if __name__ == "__main__":
    conn = get_connection()
    if conn:
        with conn.cursor() as cursor:
            cursor.execute("SHOW DATABASES;")
            databases = cursor.fetchall()
            print("database list:")
            for db in databases:
                print(db)
        conn.close()