# Query 1: Get user's chatrooms
async def get_chatrooms_from_MySQL(cursor, username):
    await cursor.execute("""
        SELECT chatroom.room_name 
        FROM chatroom
        JOIN belonging ON chatroom.room_id = belonging.room_id
        JOIN user ON belonging.user_id = user.user_id
        WHERE user.user_name = %s;
    """, (username,))
    
    result = await cursor.fetchall()
    return [row["room_name"] for row in result]

# Query 2: Get chatroom members
async def get_chatroom_members_from_MySQL(cursor, chatroom):
    await cursor.execute("""
        SELECT user.user_name
        FROM user
        JOIN belonging ON user.user_id = belonging.user_id
        JOIN chatroom ON belonging.room_id = chatroom.room_id
        WHERE chatroom.room_name = %s;
    """, (chatroom,))

    result = await cursor.fetchall()
    return [row["user_name"] for row in result]

# Query 3: Get chatroom message history
async def get_chat_history_from_MySQL(cursor, chatroom):
    await cursor.execute("""
        SELECT user.user_name, messages.content, messages.timestamp
        FROM messages
        JOIN user ON messages.sender_id = user.user_id
        JOIN chatroom ON messages.room_id = chatroom.room_id
        WHERE chatroom.room_name = %s
        ORDER BY messages.timestamp ASC;
    """, (chatroom,))

    result = await cursor.fetchall()
    return [{"user": row["user_name"], "message": row["content"], "timestamp": row["timestamp"]} for row in result]