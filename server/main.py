# websocket 

import asyncio
import websockets

async def echo(websocket, path):
    print("A client just connected")
    try:
        async for message in websocket:
            print("Received message from client: " + message)
            response = "Server received your message: " + message
            await websocket.send(response)
            print("Sent response to client")
    except websockets.exceptions.ConnectionClosed as e:
        print("A client just disconnected")

async def main():
    async with websockets.serve(echo, "localhost", 8000):
        print("Server started at ws://localhost:8000")
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())