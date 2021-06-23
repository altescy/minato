class MinatoException(Exception):
    """MinatoException"""


class ConfigurationError(MinatoException):
    """ConfigurationError"""


class CacheNotFoundError(MinatoException):
    """CacheNotFoundError"""


class InvalidCacheStatus(MinatoException):
    """InvalidCacheStatus"""


class CacheAlreadyExists(MinatoException):
    """CacheAlreadyExists"""
