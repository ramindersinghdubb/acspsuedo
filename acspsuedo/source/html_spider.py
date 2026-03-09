"""
Functions for accessing HTML contents of tree-like
databases.

Curated with a particular eye to the United States
Census Bureau's API.
"""

import asyncio
import typing as t
import pandas as pd
from html.parser import HTMLParser
import logging

from acspsuedo.source.callables import make_list
from acspsuedo.source.protocols import (
    runtime_decorator,
    batch_GET_text_io
)

logger = logging.getLogger(__name__)


@runtime_decorator
async def html_spider(
    url: str,
    html_attributes: t.Optional[t.Union[t.List[str], str]] = None,
    sleep: float = 0,
    **kwargs
) -> pd.DataFrame:
    """
    Parse a tree-like webpage and return information on all
    subdirectories/files in the form of a :py:class:`pandas.DataFrame`
    object.

    *A quick note*: The underlying context manager to send
    asynchronous requests is :py:class:`aiohttp.ClientSession`.
    This helps speed up performance without needlessly creating
    multiple sessions per GET protocol. Moreover, it has nice
    compatibility features with the built-in `asyncio` library
    (which is used here too).

    Parameters
    ----------
    url
        The url to the directory.
    
    html_attributes
        HTML attributes/words to target.

        By default, 'None' returns `['[DIR]', '[   ]', [TXT]]`.
        These attributes are specifically for the Census Bureau's
        TIGER database, where subfolders are indicated by '[DIR]'
        and files (of any kind) are indicated by '[   ]' or '[TXT]'.

    sleep
        Rate limit requests to each branch. Default `0`.

    kwargs
        Any arguments passed onto :py:class:`aiohttp.ClientSession`.

    Returns
    -------
        :py:class:`pandas.DataFrame` object containing three columns:
        'BRANCH', specifying the relative parent folder of the
        file/folder object in question; 'NODE', specifying the
        file/folder object, and; 'TYPE', indicating whether the
        aforementioned is a folder (i.e. 'BRANCH') or file (i.e.
        'LEAF').
    """
    uhp = _UrlHTMLParser()
    
    async def _html_spider(
        url: str, html_attributes: t.Optional[t.Union[t.List[str], str]] = None, **kwargs
    ) -> t.Tuple[t.List[str], t.List[str], t.List[str]]:
        html_attributes = _HTML_attrs(html_attributes)

        await asyncio.sleep(sleep)

        root        = await batch_GET_text_io(urls = url, make_coroutine = True, **kwargs)
        parsed_html = [txt_line for txt_line in root if
                       any(attr_tag in txt_line for attr_tag in html_attributes)]
        parsed_html = '\n'.join(parsed_html)
        
        branches, nodes, types, tmps = uhp.feed_database(parsed_html, url)
        await asyncio.gather(*[_html_spider(tmp) for tmp in tmps])

        return branches, nodes, types
    
    branches, nodes, types = await _html_spider(url, html_attributes, **kwargs)

    df = pd.DataFrame(
        {
            "BRANCH": branches,
            "NODE": nodes,
            "TYPE": types
        }
    )

    for i in [i for i in dir(uhp) if isinstance(getattr(uhp, i), list)]:
        eval('uhp.{}.clear()'.format(i))

    return df


class _UrlHTMLParser(HTMLParser):
    def __init__(
        self, *, convert_charrefs: bool = True
    ) -> None:
        super().__init__(convert_charrefs=convert_charrefs)

        self._nodes    = []
        self._branches = []
        self._types    = []
        self._tmp      = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        for attr, spec in attrs:
            if spec and attr == 'href':
                URL = f'{self.ROOT_URL}{spec}'
                self._branches.append(self.ROOT_URL)
                self._nodes.append(URL)
                if spec.endswith('/'):
                    self._tmp.append(URL)
                    self._types.append('BRANCH')
                else:
                    self._types.append('LEAF')

    def feed_database(
        self,
        data: str,
        root_url: t.Optional[str] = None
    ) -> t.Tuple[t.List[str], t.List[str], t.List[str], t.List[str]]:
        self._tmp.clear()

        self.ROOT_URL = root_url
        super().feed(data)
        return self._branches, self._nodes, self._types, self._tmp



def _HTML_attrs(
    attributes: t.Optional[t.Union[t.List[str], str]] = None
) -> t.List[str]:
    if attributes is None:
        attributes = ['[DIR]', '[   ]', '[TXT]']
    return make_list(attributes)