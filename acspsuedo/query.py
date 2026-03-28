"""
Main-level entry point for downloading Census Bureau data.

Note that it is recommended to obtain an API key if you are
making many (500+) queries in a daily session. An API key
is free to obtain at: https://api.census.gov/data/key_signup.html.
"""

import typing as t

import pandas as pd
import geopandas as gpd
import numpy as np


from acspsuedo.geog import GeoSpecFmtter, ApiKeyConfig
from acspsuedo.source.low.protocols import fetch_table, batch_fetch_content
from acspsuedo.source.cache import VariableCache
from acspsuedo.source.na_values import REPLACEMENT_VALUES



variable_cache: VariableCache = VariableCache()
"""
Internal source for caching metadata information regarding tables and
variables across all American Community Survey datasets.

This is exposed here in case you wish to customize caching preferences
and/or view any information regarding variables/tables via the methods
of this instance.
"""

api_key_config = ApiKeyConfig()
"""
Configuration settings for the API key.

Note that you can specify the location of your API key through one of two ways:
- By setting it in the operating system environment (prioritized)
- By writing it in a textfile (`./api_key.txt`) in the working directory.

You can customize the locations of either settings with the respective attributes:
- `api_key_config.OS_ENV_LOCATION`, for the operating system location
- `api_key_config.FILE_PATH`, for the file path location
"""

check_path_existence  = GeoSpecFmtter.check_path_existence
view_geographic_paths = GeoSpecFmtter.view_geographic_paths




def download(
    dataset: str,
    year: int,
    *,
    variables: t.Optional[t.Union[t.List[str], str]] = None,
    tables: t.Optional[t.Union[t.List[str], str]] = None,
    drop_annotation_variables: bool = True,
    convert_to_na: bool = True,
    include_geometries: bool = False,
    **geographic_specifiers
) -> t.Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Download data from the United States Census Bureau's American Community Survey's (ACS) for
    some geographies of interest.

    Note that you can specify particular variables, tables, or some combination of the two.

    Parameters
    ----------
    dataset
        A supported ACS dataset.
        
        To view the list of supported datasets, as well as their respectively available
        years, see `acspsuedo.datasets`.

    year
        A calendar year for the ACS dataset.

        Note that this calendar year must be available for the specified ACS dataset
        of interest.

    variables
        A variable, or list of variables, to be queried from the ACS dataset.

    tables
        A dataset table, or list of tables, which must be supported by the ACS dataset of interest.

    drop_annotation_variables
        The Bureau often attaches supplementary, non-required attribute and margin-of-error information
        for estimate data. Indicate whether or not to drop this information. Default `True`.
        
    convert_to_na
        Indicate whether or not special values should be replaced with `np.nan` values. Default `True`.

    include_geometries
        TODO: Indicate whether or not to incorporate geometric shapefile information. Default `False`.

    geographic_specifiers
        A set of geographic specifiers specifying the geographies on which queried data
        should be restricted to.

        To view available fully-specified geographic paths for an ACS dataset, reference the
        `~view_geographic_paths()` function. If you know your geographic specifier(s), reference
        the `~check_path_existence()` function to see whether or not they are supported for an ACS
        dataset of interest and, if they are supported, all geographic paths containing those
        specifiers of interest.

    Returns
    -------
    A :py:class:`pandas.DataFrame` containing the queried American Community Survey data
    of interest.

    If `add_geometries` is `'True'`, the return is a :py:class:`geopandas.GeoDataFrame` containing
    additional geometric shapefile information.

    Notes
    -----
    For multiple queries (500+) in a session, it is recommended to obtain an API key. API keys are
    free to obtain at https://api.census.gov/data/key_signup.html. If you wish to specify an API key,
    set the key in your operating system's environment, e.g.,
    ```
    import os
    os.environ['CENSUS_BUREAU_API_KEY'] = your_api_key_here
    ```
    or write it to a textfile in the working directory (`./api_key.txt`). If both are supplied, the
    OS environment key is prioritized.
     
    The configuration for the locations of these settings can be customized.
    ```
    from acspsuedo.query import api_key_config
    api_key_config.FILE_PATH = 'location/to/new_file_path.txt' # <- Set a custom filepath containing the key
    api_key_config.OS_ENV_LOCATION = 'new_env_key' # <- Set a custom environment location to the key
    ```
    """
    
    urls, meta_d = _fmt_download_url(
        dataset = dataset,
        year = year,
        vars = variables,
        tbls = tables,
        drop_annotation_vars = drop_annotation_variables,
        **geographic_specifiers
    )
    df = fetch_table(urls)
    df = _df_cleaner(df, year, meta_d, convert_to_na, **geographic_specifiers)

    return df


async def async_download(
    dataset: str,
    year: int,
    *,
    variables: t.Optional[t.Union[t.List[str], str]] = None,
    tables: t.Optional[t.Union[t.List[str], str]] = None,
    drop_annotation_variables: bool = True,
    convert_to_na: bool = True,
    retry_rate: int = 30,
    timeout_rate: t.Union[float, int] = 0.1,
    include_geometries: bool = False,
    **geographic_specifiers
) -> t.Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    **EXPERIMENTAL**

    Asynchronous implementation for downloading data from the United States Census Bureau's
    American Community Survey's (ACS) datasets. Execution unit is that of a concurrent model
    (since fetches are I/O-bound tasks; thread safety ensured).

    Note that you can specify particular variables, tables, or some combination of the two.

    Parameters
    ----------
    dataset
        A supported ACS dataset.
        
        To view the list of supported datasets, as well as their respectively available
        years, see `acspsuedo.datasets`.

    year
        A calendar year for the ACS dataset.

        Note that this calendar year must be available for the specified ACS dataset
        of interest.

    variables
        A variable, or list of variables, to be queried from the ACS dataset.

    tables
        A dataset table, or list of tables, which must be supported by the ACS dataset of interest.

    drop_annotation_variables
        The Bureau often attaches supplementary, non-required attribute and margin-of-error information
        for estimate data. Indicate whether or not to drop this information. Default `True`.
        
    convert_to_na
        Indicate whether or not special values should be replaced with `np.nan` values. Default `True`.

    retry_rate
        In case of server-based blocking, indicate how many attempts should be made per URL before skipping.
        Default 30.

    timeout_rate
        In case of querying large amounts of tables/variables, by how much (in seconds) should each request
        attempt be delayed by. Default 0.1 seconds.

    include_geometries
        TODO: Indicate whether or not to incorporate geometric shapefile information. Default `False`.

    geographic_specifiers
        A set of geographic specifiers specifying the geographies on which queried data
        should be restricted to.

        To view available fully-specified geographic paths for an ACS dataset, reference the
        `~view_geographic_paths()` function. If you know your geographic specifier(s), reference
        the `~check_path_existence()` function to see whether or not they are supported for the ACS
        dataset of interest and, if they are supported, all geographic paths containing those
        specifiers of interest.

    Returns
    -------
    A :py:class:`pandas.DataFrame` containing the queried American Community Survey data
    of interest.

    If `add_geometries` is `'True'`, the return is a :py:class:`geopandas.GeoDataFrame` containing
    additional geometric shapefile information.

    Notes
    -----
    For multiple queries (500+) in a session, it is recommended to obtain an API key. API keys are
    free to obtain at https://api.census.gov/data/key_signup.html. If you wish to specify an API key,
    set the key in your operating system's environment, e.g.,
    ```
    import os
    os.environ['CENSUS_BUREAU_API_KEY'] = your_api_key_here
    ```
    or write it to a textfile in the working directory (`./api_key.txt`). If both are supplied, the
    OS environment key is prioritized.
     
    The configuration for the locations of these settings can be customized.
    ```
    from acspsuedo.query import api_key_config
    api_key_config.FILE_PATH = 'location/to/new_file_path.txt' # <- Set a custom filepath containing the key
    api_key_config.OS_ENV_LOCATION = 'new_env_key' # <- Set a custom environment location to the key
    ```
    """

    # This does the heavy lifting prior to sending requests.  This is the reason
    # why thread safety is ensured since metadata formatting requires caching, but
    # because it has been implemented as a synchronous routine, and runs precisely
    # once to generate the urls/metadata, the cache is not modified as queries are
    # being fetched.
    # Moreover, the execution unit is a concurrent model, since fetches are I/O bound,
    # so the issue of thread safety should absolutely be of no concen.
    urls, meta_d = _fmt_download_url(
        dataset = dataset,
        year = year,
        vars = variables,
        tbls = tables,
        drop_annotation_vars = drop_annotation_variables,
        **geographic_specifiers
    )
    df = await batch_fetch_content(urls, retry_rate, timeout_rate)
    df = _df_cleaner(df, year, meta_d, convert_to_na, **geographic_specifiers)

    return df



def _df_cleaner(
    df: pd.DataFrame,
    year: int,
    meta_dict: t.Dict[t.Any, t.Any],
    drop_na: bool = True,
    **geo_specifiers
) -> pd.DataFrame:
    """
    Internal dataframe cleaner for cleaning queried Census Bureau data.

    Parameters
    ----------
    meta_dict
        A dictionary containing data types for each of the queried variables.
    
    drop_na
        Indicate whether or not special values should be replaced with `np.nan` values.
        Default `True`.

    geo_specifiers
        A set of geographic specifiers specifying the geographies on which queried data
        should be restricted to.
    """

    # Create a year column
    df['YEAR'] = year

    # Upper case columns
    df.columns = [col.upper() for col in df.columns]

    # Move identifier columns to the front
    geo_col_labs = GeoSpecFmtter.get_geo_cols(**geo_specifiers)
    id_cols = [col for col in ['NAME', 'GEO_ID', 'UCGID', *geo_col_labs, 'YEAR']
               if col in list(df.columns)]
    data_cols = sorted([col for col in list(df.columns) if col not in id_cols])
    df = df[id_cols + data_cols]

    # Drop duplicate columns
    df = df.iloc[:, ~df.columns.duplicated()].copy()

    # If found, sort by GEO_ID. Else, sort by id columns.
    if 'GEO_ID' in df.columns:
        df.sort_values(by = 'GEO_ID', ignore_index=True, inplace=True)
    else:
        if 'NAME' in id_cols:
            id_cols.remove('NAME')
        df.sort_values(by = id_cols, ignore_index=True, inplace=True)

    # Set column dtypes
    df = variable_cache._set_dtypes(df, meta_dict)

    # Drop NA values (if specified; default 'True')
    if drop_na:
        df.replace(REPLACEMENT_VALUES, np.nan, inplace = True)

    return df




def _fmt_download_url(
    dataset: str,
    year: int,
    vars: t.Optional[t.Union[t.List[str], str]] = None,
    tbls: t.Optional[t.Union[t.List[str], str]] = None,
    drop_annotation_vars: bool = True,
    **geog_specifiers  
) -> t.Tuple[list[str], t.Dict[str, str]]:
    """
    Internal for formatting multiple download links to the Census Bureau
    (in the potential case that a user may query 50+ variables at once).
    """
    url = _fmt_url(dataset, year, **geog_specifiers)
    vars, meta_dict = variable_cache._vars_metadata(dataset, year, vars, tbls, drop_annotation_vars)

    urls = [url.format(','.join(vars[i:i+50]) ) for i in range(0, len(vars) + 1, 50) ]

    return urls, meta_dict


def _fmt_url(dataset: str, year: int, **geog_specifiers):
    """
    Formatter skeleton for the URLs.
    """
    geo_specs = GeoSpecFmtter.get_fmt_path(dataset, year, **geog_specifiers)
    url_fmtter = 'https://api.census.gov/data/{year}/{dataset}?get={var}{geo_specs}{key}'

    fmt_url = url_fmtter.format(
        var       = '{}',
        dataset   = dataset,
        year      = str(year),
        geo_specs = geo_specs,
        key       = api_key_config._get_api_key()
    )
    
    return fmt_url