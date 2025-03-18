# Query 1: Get user's chatrooms
async def get_chatrooms_from_MySQL(cursor, username):
    await cursor.execute("""
        SELECT chatrooms.room_name 
        FROM chatrooms
        JOIN user_chatrooms ON chatrooms.room_id = user_chatrooms.room_id
        JOIN users ON user_chatrooms.user_id = users.user_id
        WHERE users.user_name = %s;
    """, (username,))
    
    result = await cursor.fetchall()
    return [row["room_name"] for row in result]

# Query 2: Get chatroom members
async def get_chatroom_members_from_MySQL(cursor, chatroom):
    await cursor.execute("""
        SELECT users.user_name
        FROM users
        JOIN user_chatrooms ON users.user_id = user_chatrooms.user_id
        JOIN chatrooms ON user_chatrooms.room_id = chatrooms.room_id
        WHERE chatrooms.room_name = %s;
    """, (chatroom,))

    result = await cursor.fetchall()
    return [row["user_name"] for row in result]

# Query 3: Get chatroom message history
async def get_chat_history_from_MySQL(cursor, chatroom):
    await cursor.execute("""
        SELECT users.user_name, messages.content, messages.timestamp
        FROM messages
        JOIN users ON messages.user_id = users.user_id
        JOIN chatrooms ON messages.room_id = chatrooms.room_id
        WHERE chatrooms.room_name = %s
        ORDER BY messages.timestamp ASC;
    """, (chatroom,))

    result = await cursor.fetchall()
    return [{"user": row["user_name"], "message": row["content"], "timestamp": row["timestamp"]} for row in result]