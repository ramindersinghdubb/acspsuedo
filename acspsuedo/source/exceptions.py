"""
Supporting exception classes.
"""

class ACSError(Exception):
    """Base class for ACS API errors."""
    pass


class TIGERShapefileError(ACSError):
    pass

class GeoScopeError(ACSError):
    pass

class FIPSError(ACSError):
    pass