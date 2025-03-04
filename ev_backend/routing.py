# routing.py
from django.urls import path
from .consumers import PostUpdateConsumer, AlertConsumer, IPBasedConsumer

websocket_urlpatterns = [
    path('ws/update/', PostUpdateConsumer.as_asgi()),
    path('ws/alerts/', AlertConsumer.as_asgi()),
    path('ws/dailyfree_update/', IPBasedConsumer.as_asgi())
]
