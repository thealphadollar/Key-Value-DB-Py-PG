class KeyDir:
    def __init__(self) -> None:
        self.items = {}
        
    def put(self, file, timestamp, key, value, offset, size):
        self.items[key] = KeyDirItem(file, timestamp, value, offset, size)
    
    def get(self, k):
        return self.items.get(k, None)

    def delete(self, k):
        del self.items[k]
        
class KeyDirItem:
    def __init__(self, file, timestamp, value, offset, size) -> None:
        self.file_id = file
        self.timestamp = timestamp
        self.value = value
        self.pos = offset
        self.size = size