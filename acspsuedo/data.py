"""
Download data from American Community Survey with the
Census Bureau's API.

Obtaining a Census Bureau API key is recommended for
requesting more than 50 datasets in a session.

*Note*: To update caching preferences, access the
`CONFIG` variable and set your preferences accordingly.
By default, caching is on and a `./cache/` is created in
the working directory.
"""

import warnings
import typing as t
from pathlib import Path
from contextlib import contextmanager, nullcontext

import pandas as pd
import geopandas as gpd
import requests as req

from acspsuedo.source import *
from acspsuedo.config import ConfigSettings
from acspsuedo.geographies import _PathFMT, _FipsFMT


CONFIG  = ConfigSettings()
FipsFMT = _FipsFMT()
PathFMT = _PathFMT()
        

@t.overload
@runtime_decorator
async def download_by_geo_collection(
    api: str,
    year: int,
    dataset: str,
    geography: str,
    *,
    api_key: t.Optional[str] = None,
    include_geometries: t.Literal[True],
    ignore_warning: bool = False,
    is_cache: t.Optional[bool] = None,
    cache_folder: t.Optional[t.Union[str, Path]] = None,
    **kwargs
) -> gpd.GeoDataFrame: ...

@t.overload
@runtime_decorator
async def download_by_geo_collection(
    api: str,
    year: int,
    dataset: str,
    geography: str,
    *,
    api_key: t.Optional[str] = None,
    include_geometries: t.Literal[False] = False,
    ignore_warning: bool = False,
    is_cache: t.Optional[bool] = None,
    cache_folder: t.Optional[t.Union[str, Path]] = None,
    **kwargs
) -> pd.DataFrame: ...

@runtime_decorator
async def download_by_geo_collection(
    api: str,
    year: int,
    dataset: str,
    geography: str,
    *,
    api_key: t.Optional[str] = None,
    include_geometries: bool = False,
    ignore_warning: bool = False,
    **kwargs
) -> t.Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Download data from the US Census Bureau's American Community Survey
    APIs for a collection of geographies (*only for years 2010 and later*).

    You can download data against different supported geographic scopes.
    
    For some geographic scopes, Federal Information Processing
    Service (FIPS) codes may be required. These can be conveniently
    found in `~/fips`.

    *Note*: A runtime decorator has been applied. This decorator
    effectively converts the coroutine into a synchronous function
    if another running event loop is not found. If another running
    event loop is found, then the output is an awaitable. Thus,
    `await`ing the result would return the output as normal.

    Parameters
    ----------
    api
        The American Community Survey (ACS) API of interest.

    year
        The year of interest.

        Note that the API be supported for this year.

    dataset
        The data table offered by the ACS API of interest.

    geography
        The geographic scope. This geography represents the collections
        of geometry (e.g. 'county' indicates queried ACS data will be
        represented against the county-level scope).

    api_key
        An API key. Note that this is recommended for querying multiple
        (50+) datasets in a session. A free API key can be requested at
        https://api.census.gov/data/key_signup.html.

    ignore_warning
        By default `False`. Infering path specifications from the supplied
        geography raises a `UserWarning` if FIPS keyword arguments are
        not supplied. This usually indicates the user wishes to collect
        data against a geography for the (default) `us` scope. `True`
        indicates such a warning should not be raised.

    include_geometries
        If `True`, fetch the corresponding geometries for the dataset in
        question. Default `False`.

    kwargs
        Keyword arguments to supply for FIPS codes.
            
        Refer to the `fips` module to view corresponding FIPS codes
        and/or references to these geographic formatters.

    Returns
    -------
        A :py:class:`pandas.DataFrame` (or a :py:class:`geopandas.GeoDataFrame`,
        if `include_geometries` is `True`) containing the queried dataset for
        the specified collection of geographies. If a running event loop is NOT
        found, the object will be an awaitable.
    """

    URL = 'https://api.census.gov/data/{year}/{base}?get=group({dataset})&ucgid=pseudo({upper}US{FIPS}${lower}){api_key}'

    upper, lower, FIPS, us_check = _fips_geo_formatters(
        api,
        year,
        geography,
        ignore_warning = ignore_warning,
        **kwargs
    )

    base = PathFMT._API_DATA[api][0]

    if api_key:
        CONFIG.api_key = api_key
    
    api_key = CONFIG.get_api_key()

    fmt_URL = URL.format(
        year = year,
        base = base,
        dataset = dataset,
        upper = upper,
        FIPS = FIPS,
        lower = lower,
        api_key = api_key
    )

    json_content = await batch_GET(fmt_URL)

    df = CONFIG._acs_df_cleaner(dataset, json_content)

    if include_geometries:
        gdf = await CONFIG.fetch_TIGER_shpfile_coro(
            geography,
            make_coroutine = True,
            _upper_scope = us_check,
            **kwargs
        )
        df = df.merge(gdf, on='GEOID')
        df = gpd.GeoDataFrame(df)

    df.drop_duplicates(inplace = True)

    return df






def view_available_datasets(
    api: str, year: int,
) -> pd.DataFrame:
    """
    View all available datasets supported by an American Community
    Survey (ACS) API for the year of interest.
    
    Parameters
    ----------
    api
        A(n) ACS API. Note that this API **must** be currently
        supported. See supported APIs by referencing the `API_DATA`
        attribute.
    
    year
        A year that **must** be supported by the specified ACS API.

    Returns
    -------
        A :py:class:`pandas.DataFrame` containing a view of all
        possible datasets accessible by the specified ACS API on the
        year of interest.
    """
    PathFMT._api_checker(api, year)

    base, _, _   = PathFMT._API_DATA[api]
    groups_URL   = f'https://api.census.gov/data/{year}/{base}/groups.json'
    json_content = req.get(groups_URL).json()
    
    groups_DF = pd.DataFrame(json_content['groups'])
    groups_DF = groups_DF[['name', 'description', 'variables']]
    groups_DF = groups_DF.rename(columns = {
        'name': 'LABEL',
        'description': 'DATASET_NAME',
        'variables': 'METADATA'
    })
    groups_DF = groups_DF.copy() # <- Needed for Pandas 3.0.0
    groups_DF['YEAR'] = year
    groups_DF['DATASET_NAME'] = [i.title() for i in groups_DF['DATASET_NAME']]
    groups_DF = groups_DF.sort_values(by = 'LABEL', ignore_index = True)
    groups_DF = groups_DF[['YEAR', 'LABEL', 'DATASET_NAME', 'METADATA']]

    return groups_DF



def view_dataset_metadata(
    api: str, year: int, dataset: str
) -> pd.DataFrame:
    """
    View metadata regarding an available dataset supported by an American
    Community Survey (ACS) API for the year of interest.

    Reference the `view_available_datasets()` function to view available
    datasets for an ACS API on the year of interest.
    
    Parameters
    ----------
    api
        A(n) ACS API. Note that this API **must** be currently
        supported. See supported APIs by referencing the `API_DATA`
        attribute.
    
    year
        A year that **must** be supported by the specified ACS API.

    dataset
        The label corresponding to the dataset of interest.

    Returns
    -------
        A :py:class:`pandas.DataFrame` containing a metadata for the
        dataset of interest.
    """
    PathFMT._api_checker(api, year)
    base, _, _ = PathFMT._API_DATA[api]

    metadata_URL = f'https://api.census.gov/data/{year}/{base}/groups/{dataset}.json'
    try:
        json_content = req.get(metadata_URL).json()
    except req.exceptions.JSONDecodeError:
        raise ValueError(
            f"'{dataset}' was not a recognizable dataset in the '{api}' API for {year}. Available "
            f"datasets for the '{api}' API in {year} can be viewed with the 'view_avail_datasets()' "
            "method."
        ) from None
    
    variables  = [dict(v, **{'VARIABLE': k}) for k, v in sorted(json_content['variables'].items())
                  if not (k.endswith('A')) and not any(k == i for i in ['NAME', 'GEO_ID', 'GEOID'])
                  ]

    metadata_DF = pd.DataFrame(variables)
    metadata_DF = metadata_DF.rename(columns = {'label': 'LABEL'})
    metadata_DF = metadata_DF.copy() # <- Needed for Pandas 3.0.0
    metadata_DF['YEAR'] = year
    metadata_DF = metadata_DF[['YEAR', 'VARIABLE', 'LABEL']]
    
    return metadata_DF


def _fips_geo_formatters(
    api: str,
    year: int,
    geography: str,
    *,
    ignore_warning: bool = False,
    **kwargs
) -> t.Tuple[str, str, str, str]:
    """
    Internal for handling formatters.

    Parameters
    ----------
    api
        The ACS API of interest.

    year
        The year of interest.

        Note that the API be supported for this year.

    geography
        The geographic scope. This geography represents the collections
        of geometry (e.g. 'county' indicates that ACS data will be
        presented for various counties).

    ignore_warning
        By default `True`. Infering the path specifications from the supplied
        geography raises a `UserWarning` if FIPS keyword arguments are
        not supplied. This usually indicates the user wishes to collect
        data against a geography for the (default) `us` scope. `False`
        indicates such a warning should not be raised.

    kwargs
        Keyword arguments to supply for FIPS codes.
            
        Refer to the `fips` module to view corresponding FIPS codes
        and/or references to these geographic formatters.
    """
    if ignore_warning:
        wcm = _ignore_warnings_cm()
    else:
        wcm = nullcontext()
    
    with wcm:
        pathspec = FipsFMT.infer_path(api, year, geography, **kwargs)

    upper, lower, FIPS, us_check = pathspec.upper_level, pathspec.lower_level, pathspec.FIPS, pathspec.upper_scope

    return upper, lower, FIPS, us_check



@contextmanager
def _ignore_warnings_cm():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield None