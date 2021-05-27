import struct
from collections import namedtuple

# https://docs.python.org/3/library/struct.html
METADATA_STRUCT = '>dqq' # bigendian, double, long long, long long
METADATA_SIZE = 3 * 8
ENCODING = 'utf-8'
Record = namedtuple(
    'Record', ['timestamp', 'keysize', 'valuesize', 'key', 'value']
)

def encode(record):
    """encode the record to bytes

    Args:
        record ([type]): [description]

    Returns:
        bytes: record converted to bytes
    """
    metadata = struct.pack(METADATA_STRUCT, record.timestamp, record.keysize, record.valuesize)
    data = record.key.encode(ENCODING) + record.value.encode(ENCODING)
    return metadata + data

def decode_metadata(metadata):
    """decode metadata from the provided metadata bytes.

    Args:
        metadata (bytes): metadata bytes read from the BitCask file

    Returns:
        tuple: timestamp, keysize, and valuesize of the record
    """
    (timestamp, keysize, valuesize) = struct.unpack(METADATA_STRUCT, metadata)
    return (timestamp, keysize, valuesize)

def decode(record_bytes):
    """decode record bytes to Record.

    Args:
        record_bytes (bytes): record bytes read from the file

    Returns:
        Record: new record from the decoded bytes
    """
    (timestamp, keysize, valuesize) = decode_metadata(record_bytes[:METADATA_SIZE])
    data_str = record_bytes[METADATA_SIZE:].decode(ENCODING)
    key = data_str[:keysize]
    value = data_str[keysize:]
    return Record(timestamp, keysize, valuesize, key, value)