class SidepocketException(Exception):
    """SidepocketException"""


class ConfigurationError(SidepocketException):
    """ConfigurationError"""


class CacheNotFoundError(SidepocketException):
    """CacheNotFoundError"""
