import hashlib


def sha256(data_string: str) -> str:
    """
    Generates a SHA-256 hash of a given string.

    Args:
        data_string (str): The input string to be hashed.

    Returns:
        str: The hexadecimal representation of the SHA-256 hash.
    """
    data_bytes = data_string.encode("utf-8")
    sha256_hash = hashlib.sha256()
    sha256_hash.update(data_bytes)
    return sha256_hash.hexdigest()
