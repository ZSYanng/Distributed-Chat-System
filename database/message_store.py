 # 处理 MySQL 存储 (增删改查)

from db import get_connection

# Create users
def create_user(username, password):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO users (username, password_hash) VALUES (%s, %s)"
            cursor.execute(sql, (username, password))
        connection.commit()
        print(f"User {username} is adding")
    except Exception as e:
        print(f"Fail to add: {e}")
    finally:
        connection.close()

# Query users
def get_users():
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT id, username FROM users")
            result = cursor.fetchall()
            return result
    except Exception as e:
        print(f"Fail to query: {e}")
        return []
    finally:
        connection.close()

# Update password
def update_password(user_id, new_password):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE users SET password_hash = %s WHERE id = %s"
            cursor.execute(sql, (new_password, user_id))
        connection.commit()
        print(f"User {user_id}'s password has been updated")
    except Exception as e:
        print(f"Fail to update: {e}")
    finally:
        connection.close()

# Delete user
def delete_user(user_id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "DELETE FROM users WHERE id = %s"
            cursor.execute(sql, (user_id,))
        connection.commit()
        print(f"User {user_id} is deleted")
    except Exception as e:
        print(f"Fail to delete: {e}")
    finally:
        connection.close()

# Test
if __name__ == "__main__":
    create_user("alice", "hashed_password_123")
    users = get_users()
    print("📌 Users list:", users)
    update_password(1, "new_hashed_password_456")
    delete_user(1)