"""
Custom exceptions supporting higher-level
implementation.
"""

class APIException(Exception):
    """Base class for API exceptions"""
    pass

class ApiKeyException(APIException):
    """Exception for missing API keys."""
    pass