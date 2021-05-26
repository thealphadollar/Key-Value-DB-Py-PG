from os import read
import uuid
import time
import main

class File:
    """[summary]
    """
    def __init__(self, dir, filename=str(uuid.uuid4()), offset=0) -> None:
        self.filename = dir + '/' + filename
        self.offset = offset
    
    def _load_next_record(self):
        """Load the next record from the file.

        Returns:
            Record: Record at the current offset.
        """
        read_bytes = 0
        with open(self.filename, 'rb') as f:
            f.seek(self.offset, 0)
            meta_bytes = f.read(main.METADATA_SIZE)
            if meta_bytes:
                (timestamp, keysize, valuesize) = main.decode_metadata(meta_bytes)
                key_bytes = f.read(keysize)
                value_bytes = f.read(valuesize)
                key = key_bytes.decode(main.ENCODING)
                value = value_bytes.decode(main.ENCODING)
                read_bytes += len(meta_bytes) + keysize + valuesize
                self.offset += read_bytes
                return main.Record(timestamp, keysize, valuesize, key, value)
    
    def write(self, key, value):
        """Appends a new Record to the file.

        Args:
            key (string): key
            value (string): value

        Returns:
            tuple: timestamp, offset after writing, size of the written data
        """
        keysize = len(key)
        valuesize = len(value)
        timestamp = time.time()
        record = main.Record(timestamp, keysize, valuesize, key, value)
        data = main.encode(record)
        count = 0
        with open(self.filename, 'ab') as f:
            count = f.write(data)
        cur_offset = self.offset
        self.offset += count
        return (timestamp, cur_offset, count)    
    
    def read(self, pos, size):
        """Read value from the file.

        Args:
            pos (int): offset of the file to start read from.
            size (int): number of bytes to read.

        Returns:
            string: value stored in the record at the provided offset.
        """
        data = b''
        with open(self.filename, 'rb') as f:
            f.seek(pos, 0)
            data = f.read(size)
        return main.decode(data).value
        