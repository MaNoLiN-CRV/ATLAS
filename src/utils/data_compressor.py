
# The data_compressor module provides functionality to compress and decompress data using zlib.
# This is useful for reducing the size of data stored in the database (sqlite)
# I'll be using a high compression level for better compression ratio.
import zlib
def compress_data(data: bytes) -> bytes:
    """
    Compresses the given data using zlib with high compression level.

    Args:
        data (bytes): The data to compress.

    Returns:
        bytes: The compressed data.
    """
    return zlib.compress(data, level=zlib.Z_BEST_COMPRESSION)

def decompress_data(data: bytes) -> bytes:
    """
    Decompresses the given data using zlib.

    Args:
        data (bytes): The compressed data to decompress.

    Returns:
        bytes: The decompressed data.
    """
    return zlib.decompress(data)