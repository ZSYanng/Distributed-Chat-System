import pymysql

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
            connect_timeout=5 
        )
        print("Connection success!")
        return connection
    except pymysql.MySQLError as e:
        print(f"Connection failure: {e}")
        return None

# Create database tables
def create_tables():
    connection = get_connection()
    if not connection:
        print("Failed to get connection.")
        return

    try:
        with connection.cursor() as cursor:
            #cursor.execute("DROP TABLE IF EXISTS messages;")
            #cursor.execute("DROP TABLE IF EXISTS belonging;")
            #cursor.execute("DROP TABLE IF EXISTS chatroom;")
            #cursor.execute("DROP TABLE IF EXISTS user;")
            # Users table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                username VARCHAR(50) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL
            );
            """)

            # Chatrooms table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS chatrooms (
                room_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                room_name VARCHAR(100) UNIQUE NOT NULL
            );
            """)

            # Users-Chatrooms relationship table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_chatrooms (
                user_id BIGINT NOT NULL,
                room_id BIGINT NOT NULL,
                PRIMARY KEY (user_id, room_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (room_id) REFERENCES chatrooms(room_id) ON DELETE CASCADE
            );
            """)
            # Messages table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                message_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                user_id BIGINT NOT NULL,
                room_id BIGINT NOT NULL,
                content TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (room_id) REFERENCES chatrooms(room_id) ON DELETE CASCADE
            );
            """)

            connection.commit()
        print("Tables created successfully!")
    except pymysql.MySQLError as e:
        print(f"Failed to create tables: {e}")
    finally:
        connection.close()

# Insert users and chatrooms and ensure every user is in each chatroom
def insert_test_data():
    connection = get_connection()
    if not connection:
        print("Database connection failed.")
        return

    try:
        with connection.cursor() as cursor:
            # Insert Users
            usernames = ['Alice', 'Mike', 'Jessie', 'David', 'Ella']
            for username in usernames:
                cursor.execute("INSERT INTO users (username, password) VALUES (%s, 'password123') ON DUPLICATE KEY UPDATE username=username;", (username,))
            
            # Insert Chatrooms
            chatrooms = ['UCI-CS', 'Technology', 'Sports']
            for room in chatrooms:
                cursor.execute("INSERT INTO chatrooms (room_name) VALUES (%s) ON DUPLICATE KEY UPDATE room_name=room_name;", (room,))

            connection.commit()

            # Get User and Chatroom IDs
            cursor.execute("SELECT user_id, username FROM users;")
            users = {row["username"]: row["user_id"] for row in cursor.fetchall()}

            cursor.execute("SELECT room_id, room_name FROM chatrooms;")
            chatrooms = {row["room_name"]: row["room_id"] for row in cursor.fetchall()}

            # Associate each user with each chatroom
            for username in usernames:
                for room in chatrooms:
                    cursor.execute("""
                        INSERT INTO user_chatrooms (user_id, room_id) VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE user_id=user_id, room_id=room_id;
                    """, (users[username], chatrooms[room]))

            connection.commit()
            print("Test data inserted successfully!")
    except pymysql.MySQLError as e:
        print(f"Error inserting test data: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    create_tables()
    insert_test_data()
