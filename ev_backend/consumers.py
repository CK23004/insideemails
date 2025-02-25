# consumers.py
import json
from channels.generic.websocket import AsyncWebsocketConsumer
from urllib.parse import parse_qs
import redis.asyncio as aioredis


class PostUpdateConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.redis = await aioredis.from_url("redis://localhost:6379/1")
        await self.channel_layer.group_add("global_progress", self.channel_name)
        await self.accept()
        print("WebSocket connected")
        await self.send_pending_updates()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard("global_progress", self.channel_name)
        await self.redis.close()
        print("WebSocket disconnected")

    async def progress_update(self, event):
        post_id = event['post_id']
        post_content = event['post_content']
        
        # Store/update the latest data for the unique post_id in Redis
        await self.redis.hset("post_updates", post_id, json.dumps({
            'post_id': post_id,
            'post_content': post_content
        }))
        
        # Push data if client is online
        await self.send(text_data=json.dumps({
            'post_id': post_id,
            'post_content': post_content
        }))
        
        # Remove the sent data
        latest_data = await self.redis.hget("post_updates", post_id)
        if latest_data and json.loads(latest_data.decode())['post_content'] == post_content:
            await self.redis.hdel("post_updates", post_id)

    async def send_pending_updates(self):
        pending_updates = await self.redis.hgetall("post_updates")
        for post_id, data in pending_updates.items():
            await self.send(text_data=data.decode())
            await self.redis.hdel("post_updates", post_id)



class AlertConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.redis = await aioredis.from_url("redis://localhost:6379/1")
        query_params = parse_qs(self.scope["query_string"].decode())
        self.session_id = query_params.get("session_id", [None])[0]

        if not self.session_id:
            await self.close()
            return

        self.group_name = f"user_{self.session_id}"
        await self.channel_layer.group_add(self.group_name, self.channel_name)
        await self.accept()
        await self.send_pending_alerts()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(self.group_name, self.channel_name)
        await self.redis.close()

    async def alert_message(self, event):
        message = event["message"]
        
        # Store the latest alert per session in Redis
        await self.redis.hset("alerts", self.session_id, json.dumps({
            'session_id': self.session_id,
            'message': message
        }))
        
        # Send the alert to the client
        await self.send(text_data=json.dumps({"message": message}))
        
        # Remove sent data
        await self.redis.hdel("alerts", self.session_id)

    async def send_pending_alerts(self):
        pending_alerts = await self.redis.hgetall("alerts")
        session_alert = pending_alerts.get(self.session_id.encode())
        if session_alert:
            await self.send(text_data=session_alert.decode())
            await self.redis.hdel("alerts", self.session_id)

