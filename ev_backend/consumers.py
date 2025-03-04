# consumers.py
import json
from channels.generic.websocket import AsyncWebsocketConsumer
from urllib.parse import parse_qs
import redis.asyncio as aioredis


# class PostUpdateConsumer(AsyncWebsocketConsumer):
#     async def connect(self):
#         self.redis = await aioredis.from_url("redis://localhost:6379/1")
#         self.queue_key = f"post_updates_queue:{self.channel_name}"
        
#         await self.channel_layer.group_add("global_progress", self.channel_name)
#         await self.accept()
#         print(f"WebSocket connected: {self.channel_name}")
        
#         await self.send_pending_updates()

#     async def disconnect(self, close_code):
#         await self.channel_layer.group_discard("global_progress", self.channel_name)
#         await self.redis.close()
#         print(f"WebSocket disconnected: {self.channel_name}")

#     async def progress_update(self, event):
#         post_id = event['post_id']
#         post_content = event['post_content']
#         update_data = json.dumps({'post_id': post_id, 'post_content': post_content})

#         # Queue the update in Redis (right-push for FIFO queue)
#         await self.redis.rpush(self.queue_key, update_data)

#         # If client is connected, send update immediately
#         if self.scope['type'] == 'websocket':
#             await self.send(text_data=update_data)

#     async def send_pending_updates(self):
#         # Drain the queue and send all pending updates
#         while True:
#             update = await self.redis.lpop(self.queue_key)
#             if update is None:
#                 break
#             await self.send(text_data=update.decode())

class PostUpdateConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.redis = await aioredis.from_url("redis://localhost:6379/1")
        self.queue_key = f"post_updates_queue:{self.channel_name}"
        
        await self.channel_layer.group_add("global_progress", self.channel_name)
        await self.accept()
        print(f"WebSocket connected: {self.channel_name}")

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard("global_progress", self.channel_name)
        await self.redis.close()
        print(f"WebSocket disconnected: {self.channel_name}")

    async def progress_update(self, event):
        post_id = event['post_id']
        post_content = event['post_content']
        update_data = json.dumps({'post_id': post_id, 'post_content': post_content})

        # Store the update in Redis queue (FIFO)
        await self.redis.rpush(self.queue_key, update_data)

    async def receive(self, text_data):
        # Client sends "poll" request to fetch updates
        data = json.loads(text_data)
        if data.get('action') == 'poll':
            await self.send_pending_updates()

    async def send_pending_updates(self):
        # Collect all updates from Redis queue
        updates = []
        while True:
            update = await self.redis.lpop(self.queue_key)
            if update is None:
                break
            updates.append(json.loads(update.decode()))

        # Send all pending updates in one go
        if updates:
            await self.send(text_data=json.dumps({'updates': updates}))

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

class IPBasedConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        # Get client IP from query params (passed directly in the WebSocket URL)
        query_params = dict((x.split('=') for x in self.scope['query_string'].decode().split('&')))
        self.client_ip = query_params.get('client_ip')

        if not self.client_ip:
            await self.close()
            return

        # Create group name based on client IP
        self.group_name = f"dailyfree_progress_{self.client_ip}"

        # Add client to their IP-based group
        await self.channel_layer.group_add(self.group_name, self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(self.group_name, self.channel_name)

    async def forward_message(self, event):
        # Send the progress update to the client
        await self.send(text_data=json.dumps({
            'email' : event['email'],
            "status" : event['status'],
         }))

    