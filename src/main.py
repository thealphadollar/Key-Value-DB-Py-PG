import struct
import bitcask_file

# TODO: Complete the docstrings, and add more comments

# https://docs.python.org/3/library/struct.html
METADATA_STRUCT = '>dqq' # bigendian, double, long long, long long
METADATA_SIZE = 3 * 8
ENCODING = 'utf-8'

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
    """[summary]

    Args:
        metadata ([type]): [description]

    Returns:
        [type]: [description]
    """
    (timestamp, keysize, valuesize) = struct.unpack(METADATA_STRUCT, metadata)
    return (timestamp, keysize, valuesize)

def decode(record_bytes):
    """[summary]

    Args:
        record_bytes ([type]): [description]

    Returns:
        [type]: [description]
    """
    (timestamp, keysize, valuesize) = decode_metadata(record_bytes[:METADATA_SIZE])
    data_str = decode_metadata[METADATA_SIZE:].decode(ENCODING)
    key = data_str[:keysize]
    value = data_str[keysize:]
    return bitcask_file.Record(timestamp, keysize, valuesize, key, value)