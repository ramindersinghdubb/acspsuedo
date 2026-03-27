"""
HTTPS GET methods/protocols.
"""
import asyncio
import typing as t
from logging import getLogger

import aiohttp
import requests
import pandas as pd

from acspsuedo.source.low.exceptions import APIException


logger = getLogger(__name__)


def fetch_content(url: str):
    """
    Synchronous method to fetch the JSON content for a(n) URL.
    """
    resp = requests.get(url)
    try:
        return resp.json()
    except requests.JSONDecodeError:
        msg = f'Error; HTTPS Status {resp.status_code}. ' + f'Response Text:\n{resp.text}'
        raise APIException(
            msg
        ) from None


def fetch_table(urls: t.List[str]) -> pd.DataFrame:
    """
    Synchronous method to fetch data from the Census Bureau.
    """

    dfs = list( map(_fetch_table, urls) )
    df = pd.concat(dfs, axis = 1)
    return df




async def batch_fetch_content(
    urls: t.Union[list[str], str],
    retry_rate: int = 30,
    timeout_rate: t.Union[float, int] = 0.1
) -> pd.DataFrame:
    """
    Asynchronous method to fetch data from the Census Bureau.
    """
    results = await _batch_fetch_content(urls, retry_rate, timeout_rate)
    urls = [r.get('url', '') for r in results]
    contents = [r.get('content', [[], []]) for r in results]

    dfs = [_census_df_fmtter(url, content) for url, content in zip(urls, contents)]
    df = pd.concat(dfs, axis = 1)
    return df


async def _batch_fetch_content(
    urls: t.Union[list[str], str],
    retry_rate: int = 30,
    timeout_rate: t.Union[float, int] = 0.1
) -> list[dict]:
    """
    Asynchronous method to fetch JSON objects (via HTTPS GET methods)
    concurrently.

    Parameters
    ----------
    urls
        One, or multiple, URLs

    retry_rate
        In case of server-based blocking, how many attempts can be
        made per URL. Default 30.

    timeout_rate
        By how much (in seconds) should each request be delayed by.
        Default 0.1.

    Returns
    -------
    A list of dictionaries containing the queried URL(s), JSON
    content (if the request was successful), and the state of the
    request.
    
    Note: If a queried URL had a successful request, the state
    would be marked as 'completed'; in cases otherwise, it would be
    marked as'interrupted' and the returned content would contain an
    error message.
    """
    async with aiohttp.ClientSession() as session:
        results = await _fetch_content(urls, session, retry_rate, timeout_rate)
        return results



async def _fetch_content(
    urls: t.Union[list[str], str],
    session: aiohttp.ClientSession,
    retry_rate: int = 30,
    timeout_rate: t.Union[float, int] = 0.1
) -> list[dict]:
    """
    The actual implementation for fetching JSON objects concurrently.

    Parameters
    ----------
    urls
        One, or multiple, URLs

    session
        An instance of :py:class:`aiohttp.ClientSession` to handle
        concurrent HTTPS GET requests.

    retry_rate
        In case of server-based blocking, how many attempts can be
        made per URL. Default 30.

    timeout_rate
        By how much (in seconds) should each request be delayed by.
        Default 0.1.
    """
    if not isinstance(urls, list):
        urls = [urls]
    
    url_tasks = [{'url': url, 'content': None, 'progress': 'not started'} for url in urls]
    
    async with session:
        await asyncio.gather(*[_get(task, session, retry_rate, timeout_rate) for task in url_tasks])
    
    return url_tasks


async def _get(
    task: dict,
    session: aiohttp.ClientSession,
    retry_rate: int = 30,
    timeout_rate: t.Union[int, float] = 0.1
):
    """
    Note that the retry_rate applies for when there are server-backed blocks,
    and the timeout_rate applies for rate-checking request attempts.
    """
    resp = await session.request('GET', task['url'])
    if resp.status == 429:
        logger.warning("%s must be restarted due to too many server requests. "
                        "Restarting...", task['url'])
        
        if 'without a key' in await resp.text():
            logger.warning("The content of %s cannot be fetched due to daily query limits without "
                            "a key. Progress status resolved as completed.", task['url'])

            task['content']  = 'Exceed daily limit for queries without an API key.'
            task['progress'] = 'completed'
        else:
            if retry_rate > 0:
                await asyncio.sleep(timeout_rate)
                task['progress'] = 'not started'
                await _get(task, session, retry_rate - 1)
            else:
                task['content'] = 'Too many requests.'
                task['progress'] = 'completed'
    
    else:
        try:
            task['content']  = await resp.json()
            task['progress'] = 'completed'
        except aiohttp.ContentTypeError as e:
            task['content']  = e.message
            task['progress'] = 'interrupted'

    await asyncio.sleep(timeout_rate)


def _fetch_table(url: str) -> pd.DataFrame:
    content = fetch_content(url)

    return _census_df_fmtter(url, content)


def _census_df_fmtter(url: str, content: t.Any) -> pd.DataFrame:
    if (
        isinstance(content, list) and
        len(content) > 1 and
        isinstance(content[0], list)
    ):
        
        upper_repl: t.Callable[[str], str] = lambda x: x.replace('(', '') \
            .replace(')', '') \
            .replace('/', '_') \
            .replace('-', '_') \
            .replace(' ', '_')
        
        df = pd.DataFrame(
            columns = [upper_repl(col) for col in content[0]],
            data    = content[1:]
        )
        return df


    raise APIException(
        f"Expected a list of lists from '{url}'. Returned content type: {type(content)}."
    )