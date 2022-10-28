from threading import Lock

from .consumer import AsyncConsumer
from ..command import CommandQueue


class Clients:
    def __init__(self):
        self._lock = Lock()
        self._clients = {}

    def add(self, client, queue: CommandQueue, consumer: AsyncConsumer):
        with self._lock:
            self._clients[client.id] = {
                "handler": client,
                "queue": queue,
                "consumer": consumer
            }

    def get(self, client):
        with self._lock:
            return self._clients[client.id]

    def remove(self, client):
        with self._lock:
            self._clients[client.id]["consumer"].stop()
            del self._clients[client.id]

    def exists(self, client_id: str) -> bool:
        with self._lock:
            return client_id in self._clients

    def __len__(self):
        with self._lock:
            return len(self._clients)
