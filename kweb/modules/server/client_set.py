from threading import Lock


class ClientSet:
    def __init__(self):
        self._lock = Lock()
        self._clients = set()

    def add(self, client):
        with self._lock:
            self._clients.add(client)
    
    def remove(self, client):
        with self._lock:
            self._clients.remove(client)

    def __len__(self):
        with self._lock:
            return len(self._clients)

