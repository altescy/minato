class MinatoException(Exception):
    """MinatoException"""


class ConfigurationError(MinatoException):
    """ConfigurationError"""


class CacheNotFoundError(MinatoException):
    """CacheNotFoundError"""
