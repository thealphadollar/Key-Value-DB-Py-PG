class KeyDir:
    """In-Memory directory to index keys for quick access.
    """
    def __init__(self) -> None:
        self.items = {}
        
    def put(self, file, timestamp, key, value, offset, size):
        """Add new item to the index.

        Args:
            file (string): file name containing the record
            timestamp (float): timestamp of the record
            key (string): key of the record
            value (string): value of the record
            offset (int): position of the storage of the record
            size (int): size (number of bytes) of the record
        """
        self.items[key] = KeyDirItem(file, timestamp, value, offset, size)
    
    def get(self, k):
        """Get entry for the key.

        Args:
            k (string): key of the required entry

        Returns:
            KeyDirItem: index entry associated with the key, None otherwise
        """
        return self.items.get(k, None)

    def delete(self, k):
        """Delete entry for the key.

        Args:
            k (string): key of the entry to be removed
        """
        del self.items[k]
        
class KeyDirItem:
    """A single item of the index directory.
    """
    def __init__(self, file, timestamp, value, offset, size):
        self.file_id = file
        self.timestamp = timestamp
        self.value = value
        self.pos = offset
        self.size = size