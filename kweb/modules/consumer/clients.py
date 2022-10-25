from threading import Lock


class Clients:
    def __init__(self):
        self._lock = Lock()
        self._clients = dict()

    def add(self, client):
        with self._lock:
            self._clients[client.id] = client
    
    def remove(self, client):
        with self._lock:
            del self._clients[client.id]

    def __len__(self):
        with self._lock:
            return len(self._clients)

