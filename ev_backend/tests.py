import asyncio
import websockets
import json

async def send_data():
    uri = "ws://localhost:8001/ws/update/"
    async with websockets.connect(uri) as websocket:
        await websocket.send(json.dumps({
            "post_id": 1,
            "content": "Test content"
        }))
        response = await websocket.recv()
        print(f"Received: {response}")

asyncio.run(send_data())


