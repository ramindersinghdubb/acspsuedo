"""
Asynchronous functions for handling GET protocols.
"""

import asyncio
import typing as t
from contextlib import asynccontextmanager
from functools import wraps
from io import StringIO

import aiohttp

from acspsuedo.source.callables import (
    make_list,
    unravel_list
)
from acspsuedo.source.exceptions import ACSError, TIGERShapefileError


import logging

logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
logger_fmt = logging.Formatter(
    fmt     = "%(name)s, %(funcName)s function (Line %(lineno)s) - %(levelname)s: %(message)s",
)
console_handler.setFormatter(logger_fmt)
logger.addHandler(console_handler)


P = t.ParamSpec("P")
R = t.TypeVar("R")


SyncFunc = t.Callable[P, R]
CoroFunc = t.Callable[P, t.Coroutine[t.Any, t.Any, R]]
GenericFunc = t.Callable[P, t.Union[R, t.Awaitable[R]]]

def runtime_decorator(func: CoroFunc[P, R]) -> SyncFunc[P, ...]:
    """
    Decorator for handling asynch functions at runtime.

    Note that if a running event loop is found and a user attempts to
    call `asyncio.run()`, a :py:class:`RuntimeError` is raised, which
    is preferential to expose to the user rather than try and raise a
    :py:class:`RuntimeWarning` depending on the loop's state (harder
    to implement, especially for nested asynch functions).

    Note as well that 'make_coroutine' is a(n) boolean added to all
    functions accepting this decorator, thereby allowing access to
    the resulting coroutine in asynchronous contexts and not raising
    a :py:class:`RuntimeError`. By default, 'make_coroutine' is
    'False'.
    """
    @wraps(func)
    def wrapper(*args, make_coroutine: bool = False, **kwargs):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug('%s: A running event loop was not found.', RuntimeWarning)
            loop = False

        if (loop and loop.is_running()) or make_coroutine:
            return func(*args, **kwargs)
        else:
            return asyncio.run( func(*args, **kwargs) )
    
    return t.cast(SyncFunc, wrapper)



@t.overload
async def batch_GET(
    urls: str, resp_method: str = 'JSON', **kwargs
) -> t.Any: ...
    
@t.overload
async def batch_GET(
    urls: t.List[str], resp_method: str = 'JSON', **kwargs
) -> t.List[t.Any]: ...

@runtime_decorator
async def batch_GET(
    urls: t.Union[str, t.List[str]], resp_method: str = 'JSON', **kwargs
) -> t.Union[t.Any, t.List[t.Any]]:
    """
    Send HTTP GET protocols for a URL or set of URLs using the
    specified `resp_method` callable (which **must** be a part
    of the :py:class:`aiohttp.ClientResponse` class and **must**
    be of :py:class:`CoroutineType`).

    *A quick note*: The underlying context manager to send
    asynchronous requests is :py:class:`aiohttp.ClientSession`.
    This helps speed up performance without needlessly creating
    multiple sessions per GET protocol. Moreover, it has nice
    compatibility features with the built-in `asyncio` library
    (which is used here too).

    Any errors, if not raised, will return a `None` value, which
    must/will be handled in upper-level functions.

    Parameters
    ----------
    urls
        One or multiple URLs.
    
    resp_method
        A callable method from :py:class:`aiohttp.ClientResponse`
        that is of :py:class:`CoroutineType`. Default is 'JSON',
        which returns a decoded JSON response.
    
    kwargs
        Any arguments passed onto :py:class:`aiohttp.ClientSession`.

    Returns
    -------
        The result(s) of the applied callable (namely, 'JSON' entails
        JSON content, 'TEXT' entails decoded response, and 'READ'
        entails the payload).
    """
    urls = make_list(urls)
    async with aiohttp.ClientSession(**kwargs) as session:
        tasks = [_async_GET_method(session, url, resp_method.lower()) for url in urls]
        return await tasks[0] if len(tasks) == 1 else await asyncio.gather(*tasks)


@t.overload
async def batch_GET_text_io(
    urls: str, **kwargs
) -> StringIO: ...
    
@t.overload
async def batch_GET_text_io(
    urls: t.List[str], **kwargs
) -> t.List[StringIO]: ...

@runtime_decorator
async def batch_GET_text_io(
    urls: t.Union[str, t.List[str]], **kwargs
) -> t.Union[StringIO, t.List[StringIO]]:
    """
    Obtain in-memory streams for text I/O (via :py:class:`io.StringIO`)
    for a URL or set of URLs.
    
    Parameters
    ----------
    urls
        One or multiple URLs.
    
    kwargs
        Any arguments passed onto :py:class:`aiohttp.ClientSession`.
    
    Returns
    -------
        One, or multiple, :py:class:`io.StringIO` in-memory streams.
    """
    contents = await batch_GET(urls, 'text', make_coroutine = True, **kwargs)
    contents = make_list(contents)
    html_contents = [StringIO(content) for content in contents]
    return unravel_list(html_contents)


@t.overload
async def batch_GET_zip(
    urls: str, **kwargs
) -> t.Any: ...
    
@t.overload
async def batch_GET_zip(
    urls: t.List[str], **kwargs
) -> t.List[t.Any]: ...

@runtime_decorator
async def batch_GET_zip(
    urls: t.Union[str, t.List[str]], **kwargs
) -> t.Union[t.Any, t.List[t.Any]]:
    """
    Obtain in-memory streams of byte I/O for a URL, or set of URLs,
    containg ZIP content.
    
    Parameters
    ----------
    urls
        One or multiple URLs.

    kwargs
        Any arguments passed onto :py:class:`aiohttp.ClientSession`.
    
    Returns
    -------
        One, or multiple, byte in-memory streams containing ZIP content.
    """
    urls = make_list(urls)
    async with aiohttp.ClientSession(**kwargs) as session:
        tasks = [_async_GET_zip(session, url) for url in urls]
        return await tasks[0] if len(tasks) == 1 else await asyncio.gather(*tasks)




@asynccontextmanager
async def _async_GET_protocol(session: aiohttp.ClientSession, url: str):
    """Async context manager for HTTP GET requests."""
    yield await session.get(url)

async def _async_GET_method(session: aiohttp.ClientSession, url: str, resp_method: str):
    """Low-level HTTP get requests (all types)."""
    async with _async_GET_protocol(session, url) as resp:
        if resp_method not in [i for i in dir(resp) if callable(getattr(resp, i))]:
            raise AttributeError("'{resp_method}' was not found and/or was not a callable.".format(resp_method = resp_method))
        try:
            resp.status
            return await eval('resp.{}()'.format(resp_method))
        except aiohttp.ClientError as e:
            error_message = await resp.text()
            raise ACSError(
                f"""Connection failed. HTTPS Status Code: {resp.status}. Error message: "{error_message}" \n"""
                f"Traceback: {e}"
            ) from None

async def _async_GET_zip(session: aiohttp.ClientSession, url: str):
    """Low-level HTTP get requests for zip content."""
    async with _async_GET_protocol(session, url) as resp:
        if resp.headers['Content-Type'] != 'application/zip':
            raise TIGERShapefileError(f"Expecting 'zipfile' type, not '{resp.headers['Content-Type']}' for '{url}'")
        try:
            return await resp.read()
        except aiohttp.ClientError as e:
            logger.error("Connection to '%s' failed. HTTPS Status Code %s. Traceback:\n%s", url, resp.status, e)