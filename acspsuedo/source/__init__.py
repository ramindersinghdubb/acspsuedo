"""
Lower-level functions to support implementation.
"""
from .protocols import (
    runtime_decorator,
    batch_GET,
    batch_GET_text_io,
    batch_GET_zip,
)
from .html_spider import html_spider
from .callables import (
    make_list,
    unravel_list,
    remove_accents,
    str_replacement
)
from .exceptions import (
    ACSError,
    TIGERShapefileError,
    GeoScopeError,
    FIPSError
)
from .cache import (
    DataCacheABC
)





__all__ = [
    "runtime_decorator",
    "batch_GET",
    "batch_GET_text_io",
    "batch_GET_zip",
    "html_spider",
    "make_list",
    "unravel_list",
    "remove_accents",
    "str_replacement",
    "ACSError",
    "TIGERShapefileError",
    "GeoScopeError",
    "FIPSError",
    "DataCacheABC"
]