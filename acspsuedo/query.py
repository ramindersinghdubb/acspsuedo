"""
Main-level entry point for downloading Census Bureau data.

Note that it is recommended to obtain an API key if you are
making many (500+) queries in a daily session. An API key
is free to obtain at: https://api.census.gov/data/key_signup.html.
"""

import typing as t
import warnings

import aiohttp
import pandas as pd
import geopandas as gpd
import numpy as np

from acspsuedo.fips import STATE_FIPS
from acspsuedo.source.geog import GeoSpecFmtter, ApiKeyConfig
from acspsuedo.source.shpfile import ShpfileFormatterException
from acspsuedo.source.shpfile_fmt import GEO_SPEC_METADATA
from acspsuedo.source.cache import VariableCache
from acspsuedo.source.na_values import REPLACEMENT_VALUES
from acspsuedo.source.low.protocols import fetch_table, batch_fetch_content
import acspsuedo.source.shpfile



api_key_config: ApiKeyConfig = ApiKeyConfig()
"""
Configuration settings for the API key.

Note that you can specify the location of your API key through one of three ways:
- By assigning it to the `API_KEY` attribute (prioritized)
- By setting it in the operating system environment
- By writing it in a textfile (`./api_key.txt`) in the working directory.

You can customize the locations of the last two settings with the respective attributes:
- `api_key_config.OS_ENV_LOCATION`, for the operating system location
- `api_key_config.FILE_PATH`, for the file path location
"""

variable_cache: VariableCache = VariableCache()
"""
Internal source for caching metadata information regarding tables and variables across
all American Community Survey datasets.

This is exposed here in case you wish to customize caching preferences and/or view any
information regarding variables/tables via the methods of this instance.
"""

shapefile_handler: acspsuedo.source.shpfile.ShpFileHandler = acspsuedo.source.shpfile.shapefile_handler
"""
Internal handler interface for TIGER shapefiles.

This is exposed here in the scenario that you may want to customize caching preferences.
There are three such caching preferences that you may customize:

- `shapefile_handler.auto_cache` (`bool`; default True)
    
    Indicate whether or not to automatically cache extracted shapefile
    information.

- `shapefile_handler.cache_path` (:py:class:`pathlib.Path` or `string`;
  default `Path.home() / 'cache' / 'acspsuedo' / 'TIGER_shapefiles'`)
    
    If `auto_cache` is True, `cache_path` specifies the caching location
    of extracted shapefiles.

- `shapefile_handler.track_updated_cache` (`bool`; default True)
    
    Indicate whether or not tracked shapefiles posited in the previous
    cache location should be moved if/when a new cache location should
    be specified. For the justification of this part of the handler
    interface, please check out the `acspsuedo.source.shpfile` module.
"""

# Two helper functions, for viewing geographic scopes metadata
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
    with_geometry_id_columns: bool = False,
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
        Indicate whether or not to incorporate geometric information from the Census Bureau's TIGER
        Shapefile database. Useful for geographical analysis and/or map visualization. Default `False`.

        Note: Due to the changes in naming conventions for TIGER shapefiles over the years or the
        non-existence of corresponding geometric information for certain scopes, this may not always
        return geometries. In which case, the return type would be a(n) :py:class:`pandas.DataFrame`
        instance containing the queried data and not the anticipated :py:class:`geopandas.GeoDataFrame`
        containing the former in addition to the respective geometric information.

    with_geometry_id_columns
        If `include_geometries` is True, indicate whether or not to append the geometric information
        with their respective identifier columns. Default `False`.

        Note: These columns have been made to cohere with those identifier variables/columns requested
        from the data query and thus are deemed redundant. Nevertheless, if you wish to specify this
        additional identifier information, you can set this setting to `True`.

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
    A :py:class:`pandas.DataFrame` containing the queried American Community Survey data of interest.

    If `add_geometries` is `'True'`, and TIGER shapefile data exists for the queried data, the return
    is a :py:class:`geopandas.GeoDataFrame` containing geometric shapefile information.

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
    
    urls, meta_d, geographic_specifiers = _fmt_download_url(
        dataset = dataset,
        year = year,
        vars = variables,
        tbls = tables,
        drop_annotation_vars = drop_annotation_variables,
        **geographic_specifiers
    )
    df = fetch_table(urls)
    df = _df_cleaner(df, year, meta_d, convert_to_na, **geographic_specifiers)

    if include_geometries:
        df = append_geographic_info(df, year, with_geometry_id_columns, **geographic_specifiers)

    return df


async def async_download(
    session: aiohttp.ClientSession,
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
    with_geometry_id_columns: bool = False,
    **geographic_specifiers
) -> t.Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Asynchronous implementation for downloading data from the United States Census Bureau's
    American Community Survey's (ACS) datasets. Execution unit is that of a concurrent model
    (since fetches are I/O-bound tasks; thread safety ensured).

    Note that you can specify particular variables, tables, or some combination of the two.

    Parameters
    ----------
    session
        A(n) :py:class:`aiohttp.ClientSession` interface/context manager.
    
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
        Indicate whether or not to incorporate geometric information from the Census Bureau's TIGER
        Shapefile database. Useful for geographical analysis and/or map visualization. Default `False`.

        Note: Due to the changes in naming conventions for TIGER shapefiles over the years or the
        non-existence of corresponding geometric information for certain scopes, this may not always
        return geometries. In which case, the return type would be a(n) :py:class:`pandas.DataFrame`
        instance containing the queried data and not the anticipated :py:class:`geopandas.GeoDataFrame`
        containing the former in addition to the respective geometric information.

    with_geometry_id_columns
        If `include_geometries` is True, indicate whether or not to append the geometric information
        with their respective identifier columns. Default `False`.

        Note: These columns have been made to cohere with those identifier variables/columns requested
        from the data query and thus are deemed redundant. Nevertheless, if you wish to specify this
        additional identifier information, you can set this setting to `True`.

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
    A :py:class:`pandas.DataFrame` containing the queried American Community Survey data of interest.

    If `add_geometries` is `'True'`, and TIGER shapefile data exists for the queried data, the return
    is a :py:class:`geopandas.GeoDataFrame` containing geometric shapefile information.

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
    urls, meta_d, geographic_specifiers = _fmt_download_url(
        dataset = dataset,
        year = year,
        vars = variables,
        tbls = tables,
        drop_annotation_vars = drop_annotation_variables,
        **geographic_specifiers
    )
    df = await batch_fetch_content(session, urls, retry_rate, timeout_rate)
    df = _df_cleaner(df, year, meta_d, convert_to_na, **geographic_specifiers)

    if include_geometries:
        df = append_geographic_info(df, year, with_geometry_id_columns, **geographic_specifiers)

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
        df['GEO_ID'] = [col.split('US', 1)[-1] for col in df['GEO_ID']]
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
) -> t.Tuple[list[str], t.Dict[str, str], t.Dict[str, str]]:
    """
    Internal for formatting multiple download links to the Census Bureau
    (in the potential case that a user may query 50+ variables at once).
    """
    url, geog_specifiers = _fmt_url(dataset, year, **geog_specifiers)
    vars, meta_dict = variable_cache._vars_metadata(dataset, year, vars, tbls, drop_annotation_vars)

    urls = [url.format(','.join(vars[i:i+50]) ) for i in range(0, len(vars) + 1, 50) ]

    return urls, meta_dict, geog_specifiers


def _fmt_url(dataset: str, year: int, **geog_specifiers):
    """
    Formatter skeleton for the URLs.
    """
    geo_specs, geog_specifiers = GeoSpecFmtter.get_fmt_path(dataset, year, **geog_specifiers)
    url_fmtter = 'https://api.census.gov/data/{year}/{dataset}?get={var}{geo_specs}{key}'

    fmt_url = url_fmtter.format(
        var       = '{}',
        dataset   = dataset,
        year      = str(year),
        geo_specs = geo_specs,
        key       = api_key_config._get_api_key()
    )
    
    return fmt_url, geog_specifiers





def append_geographic_info(
    data_df: pd.DataFrame,
    year: int,
    with_geometry_columns: bool = False,
    **geographic_specifiers: t.Any
) -> t.Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Add geographic data to the queried dataset.

    Parameters
    ----------
    data_df
        The returned :py:class:`pandas.DataFrame` instance generated from the
        query.

    year
        The calendar year for the queried data.

    with_geometry_columns
        Indicate whether or not to attach any

    geographic_specifiers
        The set of geographic specifiers that were used from the query.

    Returns
    -------
    A :py:class:`geopandas.GeoDataFrame` instance containing geographic information
    for each record/row from the fetched Census Bureau data *provided a shapefile
    is found*. Otherwise, the originally queried data is returned alongside a helpful
    warning.
    """
    gdf = _get_shpfile(data_df, year, **geographic_specifiers)
    
    if gdf.empty:
        # Suggests shapefile non-existence and/or naming convention issues.
        # Because of our internal handler set-up, a warning is automatically raised.
        return data_df
    
    shpfile_scope = list(geographic_specifiers)[-1]
    _, merge_df_cols, _, merge_gdf_cols, _ = GEO_SPEC_METADATA[shpfile_scope]

    if 'GEO_ID' in data_df.columns and 'GEO_ID' in gdf.columns:
        merge_df_cols = ['GEO_ID', *merge_df_cols]
        merge_gdf_cols = ['GEO_ID', *merge_gdf_cols]

    merge_gdf = pd.merge(
        right  = data_df,
        left   = gdf,
        how    = "right",
        right_on = [*merge_df_cols, 'YEAR'],
        left_on  = [*merge_gdf_cols, 'YEAR'],
    )

    # Move the queried data columns to the front
    df_cols = list(data_df.columns)
    gdf_cols = [col for col in merge_gdf.columns if col not in [*df_cols, 'geometry']]
    
    merge_gdf = merge_gdf[
        df_cols + (gdf_cols if with_geometry_columns else []) + ['geometry']
    ]

    return merge_gdf



def _get_shpfile(
    df: pd.DataFrame,
    year: int,
    **geographic_specifiers: t.Any
) -> gpd.GeoDataFrame:
    """
    Underlying that makes runs to the shapefile database (if files are not previously
    cached) with our shapefile handler and formats any shapefiles requiring a 'state'
    outer point of reference.
    """
    try:
        gdf = shapefile_handler.fetch_tiger_shpfile(year, **geographic_specifiers)
        if gdf is None:
            # Indicates our anticipated naming convention issues or shapefile non-existence
            return gpd.GeoDataFrame()
        return gdf
   
   # Specific handling for shapefiles whose outer point of reference is 'state'
    except ShpfileFormatterException:
        
        # If 'STATE' is found, great. Otherwise, we load all 'state' FIPS codes and
        # extract the shapefiles from each. The latter typically arises when users
        # run data queries for certain geographic pathways of length 1 and/or containing
        # wildcard operators (e.g. {'congressional_district': '*'}).
        if 'STATE' in df.columns:
            states = list(df['STATE'].unique())
        else:
            states = list(STATE_FIPS.values())
        
        # Shapefile scope governed by last specifier
        shpfile_scope = list(geographic_specifiers)[-1]
        
        gdfs = []
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            for state in states:
                gdf = shapefile_handler.fetch_tiger_shpfile(year, **{'state': state, shpfile_scope: '*'})
                if gdf is not None:
                    gdfs.append(gdf)

        gdf = pd.concat(gdfs, ignore_index = True)
        return gdf
    


def confined_download(area_threshold: t.Union[int, float] = 0.7, **geographic_specifiers):
    """
    Download data from the United States Census Bureau's American Community Survey's (ACS) for
    geographies of interest that are confined to the outer-layer of geographies specified here.

    Parameters
    ----------
    area_threshold
        What percentage of the inner-layer set of geographies' areas must be within the outer-layer
        geography area. Default 0.7.

    geographic_specifiers
        A set of geographic specifiers specifying the geographies on which queried data, for the
        inner-layer set of geographies, should be confined within.

        To view available fully-specified geographic paths for an ACS dataset, reference the
        `~view_geographic_paths()` function. If you know your geographic specifier(s), reference
        the `~check_path_existence()` function to see whether or not they are supported for an ACS
        dataset of interest and, if they are supported, all geographic paths containing those
        specifiers of interest.

    Returns
    -------
    A :py:class:`acspsuedo.query._ConfinedDownload` instance.

    *Note*: This class supports a (synchronous) download method, whose parameter space is the same
    as that of the normal `acspsuedo.query.download()` function.
    """
    if (0 > area_threshold) or (area_threshold > 1):
        raise ValueError("Valid area threshold values must be between 0 and 1.")
    return _ConfinedDownload(area_threshold, **geographic_specifiers)



class _ConfinedDownload:
    """
    Handler for downloading ACS data at geographic specifiers that are
    confined to a different scope than readily permissible.
    """
    def __init__(self, area_threshold: t.Union[int, float], **geograhic_specifiers) -> None:
        self._area_threshold        = area_threshold
        self._geographic_specifiers = geograhic_specifiers

        # Internals for:
        # - Checking if a query attempt has been made, and
        # - The outer-layer geography (if an attempt is successful)
        self._query_attempt   = False
        self._outer_geography = None

    @property
    def area_threshold(self):
        """
        The percentage of the inner-layer of geographies' areas that must be within the
        outer-layer geography area.
        """
        return self._area_threshold
    
    @area_threshold.setter
    def area_threshold(self, new_threshold: t.Union[int, float]):
        if (0 > new_threshold) or (new_threshold > 1):
            raise ValueError("Valid area threshold values must be between 0 and 1.")
        self._area_threshold = new_threshold
    
    @property
    def geographic_specifiers(self):
        """
        The geographic specifiers indicating the outer-layer geography area to which
        inner-level geographies from queried data will be confined to.
        """
        return self._geographic_specifiers
    
    @geographic_specifiers.setter
    def geographic_specifiers(self, new_specififers: t.Dict[t.Any, t.Any]):
        # Reset query attempt state.
        if self._geographic_specifiers != new_specififers:
            self._query_attempt = False
        self._geographic_specifiers = new_specififers
    
    def __repr__(self) -> str:
        return "_ConfinedDownload(area_threshold = {}, geographic_specifiers = {{{}}})".format(
            self._area_threshold,
            ', '.join([f"{k} = {v}" for k, v in self._geographic_specifiers.items()])
        )
    
    def __eq__(self, other) -> bool:
        if isinstance(other, _ConfinedDownload):
            return (self._area_threshold == other._area_threshold) and \
                (self._geographic_specifiers == other.geographic_specifiers)
        return False
    
    def download(
        self,
        dataset: str,
        year: int,
        *,
        variables: t.Optional[t.Union[t.List[str], str]] = None,
        tables: t.Optional[t.Union[t.List[str], str]] = None,
        drop_annotation_variables: bool = True,
        convert_to_na: bool = True,
        include_geometries: bool = False,
        with_geometry_id_columns: bool = False,
        **geographic_specifiers
    ) -> t.Union[pd.DataFrame, gpd.GeoDataFrame]:
        """
        Download data from the United States Census Bureau's American Community Survey's (ACS) for
        geographies of interest that are confined to the outer-layer of geographies specified from
        this function.

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
            Indicate whether or not to incorporate geometric information from the Census Bureau's TIGER
            Shapefile database. Useful for geographical analysis and/or map visualization. Default `False`.

            Note: Due to the changes in naming conventions for TIGER shapefiles over the years or the
            non-existence of corresponding geometric information for certain scopes, this may not always
            return geometries. In which case, the return type would be a(n) :py:class:`pandas.DataFrame`
            instance containing the queried data and not the anticipated :py:class:`geopandas.GeoDataFrame`
            containing the former in addition to the respective geometric information.

        with_geometry_id_columns
            If `include_geometries` is True, indicate whether or not to append the geometric information
            with their respective identifier columns. Default `False`.

            Note: These columns have been made to cohere with those identifier variables/columns requested
            from the data query and thus are deemed redundant. Nevertheless, if you wish to specify this
            additional identifier information, you can set this setting to `True`.

        geographic_specifiers
            The set of inner-layer geographic specifiers indicating the geographies to which queried data
            references. Queried data at this inner-layer, in turn, will be confined to the outer-layer
            geography.

            To view available fully-specified geographic paths for an ACS dataset, reference the
            `~view_geographic_paths()` function. If you know your geographic specifier(s), reference
            the `~check_path_existence()` function to see whether or not they are supported for an ACS
            dataset of interest and, if they are supported, all geographic paths containing those
            specifiers of interest.

        Returns
        -------
        A :py:class:`pandas.DataFrame` containing the queried American Community Survey data of interest.

        If `add_geometries` is `'True'`, and TIGER shapefile data exists for the queried data, the return
        is a :py:class:`geopandas.GeoDataFrame` containing geometric shapefile information.

        Notes
        -----
        An empty :py:class:`pandas.DataFrame` (or :py:class:`geopandas.GeoDataFrame`, if `add_geometries`
        is `'True'`) may be returned. This corresponds to a scenario in which there are no inner-layer
        geographies so much as touching the border of the outer-layer geography.

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
        inner_data = download(
            dataset = dataset,
            year = year,
            variables = variables,
            tables = tables,
            drop_annotation_variables = drop_annotation_variables,
            convert_to_na = convert_to_na,
            with_geometry_id_columns = with_geometry_id_columns,
            include_geometries = True,
            **geographic_specifiers
        )
        # To accomodate for cases when a TIGER shapefile cannot be found (thanks
        # to our earlier configurations, a warning is automatically raised)
        if not isinstance(inner_data, gpd.GeoDataFrame): 
            return inner_data
        
        inner_crs = inner_data.crs # <- Keep the original Coordinate Referencing System

        outer_data = self._get_outer_download(dataset, year, variables, tables)

        if outer_data is None:
            msg = \
            f"\nCould not locate the appropriate TIGER shapefile for the outer-layer set " \
            f"of geographies given by {self._geographic_specifiers} for the {year} calendar\n" \
            f"year.\n" \
            "\nAs a result, the returned set of data corresponds to data downloaded solely from " \
            "the reference of the inner-layer set of geographic specifiers."

            warnings.warn( msg, UserWarning )
            return inner_data
        
        # Necessary to avoid modifying the referenced object
        outer_data = outer_data.copy()

        confined_data = self.__confined_data_fmtter(inner_data, outer_data, inner_crs, include_geometries)

        return confined_data
    
    def __confined_data_fmtter(
        self,
        inner_data: gpd.GeoDataFrame,
        outer_data: gpd.GeoDataFrame,
        inner_data_crs: t.Any,
        include_geometries: bool
    ) -> t.Union[pd.DataFrame, gpd.GeoDataFrame]:
        # Project to Web-Mercator
        inner_data.to_crs(3857, inplace=True)
        outer_data.to_crs(3857, inplace=True)

        # Keep the geometries
        inner_data['inner_geometry'] = inner_data.geometry
        outer_data['outer_geometry'] = outer_data.geometry

        # Confining
        confined_data = inner_data.sjoin(
            outer_data,
            how = 'inner',
            predicate = 'intersects',
            lsuffix = 'inner',
            rsuffix = 'outer'
        )

        # Thresholding
        thresholded_data = confined_data[
            confined_data['inner_geometry'].intersection(confined_data['outer_geometry']).area >=
            self._area_threshold * confined_data['inner_geometry'].area
        ]

        # Cleaning (to ensure consistency w/o confinement)
        thresholded_data.drop(columns = ['inner_geometry', 'outer_geometry',
                                         *[col for col in thresholded_data if col.endswith('_outer')]],
                              inplace = True)
        thresholded_data.columns = [col.rstrip('_inner') for col in thresholded_data.columns]
        thresholded_data.reset_index(drop = True, inplace=True)

        # Restore to the original/inner CRS
        thresholded_data.to_crs(inner_data_crs, inplace=True)

        # Drop the geometry column (if indicated False)
        if not include_geometries:
            thresholded_data.drop(columns = ['geometry'], inplace = True)

        return thresholded_data
        

    
    def _get_outer_download(
        self,
        dataset: str,
        year: int,
        variables: t.Optional[t.Union[t.List[str], str]] = None,
        tables: t.Optional[t.Union[t.List[str], str]] = None,
    ) -> t.Optional[gpd.GeoDataFrame]:
        """Internal for retrieving the outer-layer geography."""
        self.__set_outer_download(dataset, year, variables, tables)
        return self._outer_geography

    def __set_outer_download(
        self,
        dataset: str,
        year: int,
        variables: t.Optional[t.Union[t.List[str], str]] = None,
        tables: t.Optional[t.Union[t.List[str], str]] = None,
    ) -> None:
        """Internal for the actual call to the outer-layer geography."""
        # Run an attempt only if we have not previous made a previous
        # attempt to query the outer-layer geographies.
        if not self._query_attempt:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                
                outer_data = download(
                    dataset = dataset,
                    year = year,
                    variables = variables,
                    tables = tables,
                    include_geometries = True,
                    **self._geographic_specifiers
                )

                # Given our downloads are confined to the outer-layer
                # of geographies, retain only the geometry column.
                if isinstance(outer_data, gpd.GeoDataFrame):
                    outer_data = outer_data[['geometry']].copy()
                
                self._outer_geography = outer_data if isinstance(outer_data, gpd.GeoDataFrame) else None
                
                # Set the query attempt to True, indicating if future queries
                # are made w/ the same set of geographic specifiers, we should
                # retrieve the stored info from the instance and don't run API
                # calls.
                self._query_attempt   = True