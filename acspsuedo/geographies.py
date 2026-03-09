"""
High-level functions and objects to support testing
and querying geographic IDs pertaining to the United
States Census Bureau's American Community Survey
API.
"""

import inspect
import re
import warnings
import typing as t
from pathlib import Path
from collections import defaultdict, namedtuple

import pandas as pd
import geopandas as gpd
import requests as req

from acspsuedo.source import *
from acspsuedo.fips import *
from acspsuedo.api import (
    API_DATA as api_data,
    _GEO_PARENT_CHILD_DICT
)


class ShapeFileHandler(DataCacheABC):
    """
    A class for handling TIGER Shapefile requesting, extraction,
    and formatting.
    """

    _GEO_SCOPES_DICT = _GEO_PARENT_CHILD_DICT

    def __init__(
        self,
        year: int = 2020,
        is_cache: t.Optional[bool] = None,
        cache_path: t.Optional[t.Union[str, Path]] = None
    ):
        """
        Initialization for :py:class:`~geographies.ShapeFileHandler`.

        Parameters
        ----------
        year
            Shapefile year.

        is_cache
            If `True`, automatically cache the extracted shapefiles. If
            `False`, only fetch the shapefiles but do not extract. By
            default, this is 'None' (evaluated as `True`).

        cache_path
            The folder path to locally cache files. By default, this is
            `None` (evaluated as `True`) and thus creates a cache folder
            at `pathlib.Path.cwd() / 'cache'` if `auto_cache` is True.
        """

        self._year       = year
        self._is_cache   = is_cache
        self._cache_path = cache_path

    @property
    def year(self) -> t.Optional[int]:
        """Year for the TIGER Shapefile."""
        return self._year
    
    @year.setter
    def year(self, new_value: int) -> None:
        self._year = new_value


    @t.overload
    @runtime_decorator
    async def fetch_TIGER_shpfile_coro(
        self,
        *args,
        make_coroutine: t.Literal[True],
        **kwargs: str
    ) -> t.Awaitable[gpd.GeoDataFrame]: ...

    @t.overload
    @runtime_decorator
    async def fetch_TIGER_shpfile_coro(
        self,
        *args,
        make_coroutine: t.Literal[False],
        **kwargs: str
    ) -> gpd.GeoDataFrame: ...
    
    @runtime_decorator
    async def fetch_TIGER_shpfile_coro(
        self,
        geography: str,
        extension: str = 'shp',
        *,
        _upper_scope: t.Optional[str] = None,
        **kwargs: str
    ) -> gpd.GeoDataFrame:
        """
        Fetch the TIGER shapefile containing collections of geometries
        for the specified geographic scope and return a(n)
        :py:class:`~geopandas.GeoDataFrame` object.

        Note: A runtime decorator has been applied. This decorator
        effectively converts the coroutine into a synchronous function
        if another running event loop is not found. If another running
        event loop is found, then the output is an awaitable. Thus,
        `await`ing the result would return the output as normal.

        Parameters
        ----------
        geography
            The geographic scope of the TIGER Shapefile.

        extension
            If caching, the TIGER shapefile path extension. By default,
            this is `'shp'`.

        _upper_scope
            Internal to check if the upper-level/parent scope is 'us'.

        kwargs
            Keyword arguments to provide if a TIGER shapefile requires
            specific geographic formatters.
             
            Refer to the `fips` module to view corresponding FIPS codes
            and/or references to these geographic formatters.

        Returns
        -------
            A(n) :py:class:`~geopandas.GeoDataFrame` object.

            Note that this object will containing latitudinal/longitudinal points,
            GEOID info, and miscelleanous FIPS info (if available). This is ideally
            formatted for either dashboard UIs (e.g. Plotly, ArcGIS) or classic
            tabular dataframe libraries (e.g. Pandas, Polars).
        """
        if _upper_scope == 'us':
            TIGER_url = list(set([self.TIGER_url_constructor(child_scope = geography, state = i) for i in
                                  list(STATE_FIPS.values()) if i != '74']))
        else:
            TIGER_url = self.TIGER_url_constructor(child_scope = geography, **kwargs)

        if self.is_cache:
            gdfs = []
            for url in make_list(TIGER_url):
                rel_path  = url.replace('.zip', '').split('/')[-1]
                file_path = f'{self.cache_path}/geometry/{rel_path}.{extension}'
                
                if Path(file_path).exists():
                    gdf = gpd.read_file(file_path)
                else:
                    gdf = await self._fetch_shpfile_coro(TIGER_url, geography, make_coroutine = True)
                gdfs.append(gdf)
            
            gdf = pd.concat(gdfs)
        
        else:
            gdf = await self._fetch_shpfile_coro(TIGER_url, geography, make_coroutine = True)
        
        return gdf
    

    def TIGER_url_constructor(
        self, 
        *,
        child_scope: str,
        **kwargs
    ) -> str:
        """
        Construct a TIGER shapefile url.

        Parameters
        ----------
        child_scope
            The child-level scope of the geometry collection.

        kwargs
            Keyword arguments to provide if a TIGER shapefile requires
            specific geographic formatters. Available formatters include:
            ['state']. Refer to the `fips` module to view corresponding
            FIPS codes and/or references to these geographic formatters.

        Returns
        -------
            The formatted TIGER shapefile url.
        """
        base_url = self._tiger_url_constructor(child_scope)
        try:
            return base_url.format( **_FipsFMT._geo_fips_fmtter(**kwargs) )
        except KeyError:
            fmtters = ShapeFileHandler._GEO_SCOPES_DICT[child_scope][3]
            raise KeyError(
                f"TIGER shapefiles for the {child_scope} scope require valid geographic formatters. "
                f"Missing geographic formatter(s): '{fmtters}'."
            ) from None
    
    @classmethod
    def _read_geo_scopes_df(cls) -> pd.DataFrame:
        """
        Returns the .CSV file containing the list of all
        available geographic scope combinations.
        """
        url = 'https://www2.census.gov/data/api-documentation/list-of-available-collections-of-geographies.xlsx'
    
        df = pd.read_excel(url)
        
        df['Geography Collection'] = df['Geography Collection'].str.strip()

        # Making string columns corresponding to the parent/child scopes
        df['Child Name'], df['Parent Name']= zip(*[i.split(' within ') for i in df['Geography Collection']])

        df = df[['Parent Name', 'Child Name',
                    'Parent Summary Level', 'Child Summary Level',
                    'Child Geography ID', 'Geography Collection']]

        # Dropping misleading combinations via child summary level
        df = df.drop(df[df['Child Geography ID'].str.contains('8610000|8710000', na = False)].index)

        df = df.sort_values(by           = ['Parent Summary Level', 'Child Summary Level'],
                            ascending    = [False, True],
                            ignore_index = True)
        return df
    
    
    @t.overload
    async def _fetch_shpfile_coro(
        self, urls: str, geography: str, extension: str = 'shp', **kwargs
    ) -> gpd.GeoDataFrame: ...

    @t.overload
    async def _fetch_shpfile_coro(
        self, urls: t.List[str], geography: str, extension: str = 'shp', **kwargs
    ) -> gpd.GeoDataFrame: ...

    @runtime_decorator
    async def _fetch_shpfile_coro(
        self, urls: t.Union[t.List[str], str], geography: str, extension: str = 'shp', **kwargs
    ) -> gpd.GeoDataFrame:
        """
        Fetch the TIGER shapefile for the given URL(s) and return
        a(n) :py:class:`~geopandas.GeoDataFrame` object.

        *A quick note*: The underlying context manager to send
        asynchronous requests is :py:class:`~aiohttp.ClientSession`.
        This helps speed up performance without needlessly creating
        multiple sessions per GET protocol. Moreover, it has nice
        compatibility features with the built-in `asyncio` library
        (which is used here too).
        
        Parameters
        ----------
        urls
            One or multiple URLs to the appropriately specified
            .ZIP files.

        kwargs
            Any arguments passed onto :py:class:`~aiohttp.ClientSession`.

        Returns
        -------
            A(n) :py:class:`~geopandas.GeoDataFrame` object.
        """
        urls = make_list(urls)
        byte_contents = await batch_GET_zip(
            urls = urls,
            make_coroutine = True,
            **kwargs
        )
        gdfs = [gpd.read_file(content) for content in make_list(byte_contents)]
        gdfs = [self._gdf_cleaner(geography, gdf) for gdf in gdfs]
        if self.is_cache:
            Path(f'{self.cache_path}/geometry').mkdir(parents=True, exist_ok=True)
            for url, gdf in zip(urls, gdfs):
                rel_path  = url.replace('.zip', '').split('/')[-1]
                file_path = f'{self.cache_path}/geometry/{rel_path}.{extension}'
                gdf.to_file(file_path)
        gdf = pd.concat(gdfs, ignore_index = True)
        return t.cast(gpd.GeoDataFrame, gdf)
    
    
    def _gdf_cleaner(self, child_scope: str, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        # Remove numbers in column labels
        gdf.columns = [re.sub(r'\d+', '', col) for col in gdf.columns]

        # Return the default and custom columns
        default_cols = ['GEOID', 'INTPTLAT', 'INTPTLON', 'geometry']
        geo_dict = ShapeFileHandler._GEO_SCOPES_DICT
        _, _, custom_cols, _, _ = geo_dict[child_scope]
        custom_cols = make_list(custom_cols)
        custom_cols = [col for col in custom_cols if col is not None]

        gdf = gdf[custom_cols + default_cols]

        # Convert the latitudinal & longitudinal center points into floats.
        for col in ['INTPTLAT', 'INTPTLON']:
            gdf = gdf.copy() # <- Needed for when Pandas is updated to 3.0.0
            gdf[col] = [float(i.replace('+', '')) for i in gdf[col]]

        return gdf
    
    def _tiger_url_constructor(
        self,
        scope: str,
    ) -> str:
        """Internal shapefile URL constructor."""
        year      = self.year
        geo_dict  = ShapeFileHandler._GEO_SCOPES_DICT
        subfolder = scope.upper()
        if scope not in geo_dict.keys():
            raise GeoScopeError(
                f"'{scope}' is not a valid scope. Supported geographic scopes include "
                f"{[k for k in geo_dict.keys() if geo_dict[k][1] is not None]}."
            )
        _, cs, _, shpfile_fmtter, shpfile_const = geo_dict[scope]

        # Quick check
        if cs is None:
            raise GeoScopeError(
                'There are no TIGER shapefiles for this scope. Supported geographic scopes with '
                f'shapefiles include: {[k for k in geo_dict.keys() if geo_dict[k][1] is not None]}'
            )

        # 2010 is the only year for which this has to be done (besides 2000, which we ignore here)
        if shpfile_const is None and year == 2010:
            subfolder += '/2010'
            scope += '10'

        # Special constructors for certain geographic scopes
        if isinstance(shpfile_const, t.Callable):
            year, subfolder, scope = shpfile_const(year)

        base_url = f'https://www2.census.gov/geo/tiger/TIGER{year}'
        path = f'{subfolder}/tl_{year}_{shpfile_fmtter}_{scope}.zip'
        
        return f'{base_url}/{path}'




class _FipsFMT:
    _FIPS_dict = defaultdict(
        lambda: ([], lambda *args: ''),
        {
            'state': (
                ['state'],
                lambda state: {**STATE_FIPS, **STATE_FIPS_ABBREV}[state]
            ),
            'county': (
                ['state', 'county'],
                lambda state, county: COUNTY[state][county]
            ),
            'place': (
                ['state', 'place'],
                lambda state, place: PLACE[state][place]
            )
        })
    
    @classmethod
    def infer_path(
        cls,
        api: str,
        year: int,
        geography: str,
        **kwargs
    ) -> "PathSpec":
        """
        Given an API, its supported year, an available (lower-level)
        geographic scoper, and optionally supplied FIPS code keyword
        arguments, infer the path specification.

        Under-determined or over-determined FIPS codes raises
        a `~FIPSError` code. A `UserWarning` is raised if a partially
        specified path is returned.

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

        kwargs
            Keyword arguments to supply for FIPS codes.
             
            Refer to the `fips` module to view corresponding FIPS codes
            and/or references to these geographic formatters.

        Returns
        -------
            A :py:class:`~PathSpec` containing detailed information on:
            
                - The upper-level/parent scope,
                
                - The lower-level/child scope,
            
                - Whether the path has any predecessing upper-level
                  requirements (e.g. `county` requires FIPS information
                  for [`state`, `county`]),
                  
                - The formatted FIPS code, and
                
                - Whether the formatted FIPS code is completely specified
                  according to the predecessing upper-level requirements
        """
        paths = _FipsFMT._list_compat_paths(api, year, geography, **kwargs)

        full_paths = [p for p in paths if p.has_require and p.is_full_spec]
        
        if len(full_paths) > 0:
            if len(full_paths) > 2:
                raise FIPSError(
                    f'Found multiple fully-specificied paths: '
                    f'{[i for i in full_paths[1:]]}'
                )
            else:
                return full_paths[-1]
        else:
            partial_paths = [p for p in paths if not p.has_require and p.is_full_spec]
            if len(partial_paths) > 1:
                raise FIPSError(
                    f"Not enough FIPS codes specified to infer a fully-specified path. "
                    f"Defaulted to infering partially-specified paths and found multiple "
                    f"such paths for '{geography}' with the following upper-level/parent "
                    f"scopes: {[p.upper_scope for p in partial_paths]}"
                )
            else:
                warnings.warn(
                    f"Could not find any fully-specificied paths. Defaulting to a partially specificied path "
                    f"with upper-level/parent scope of '{partial_paths[0].upper_scope}'",
                    UserWarning,
                    stacklevel = 2
                )
                return partial_paths[0]


    
    @classmethod
    def _list_compat_paths(
        cls, api: str, year: int, geography: str, **kwargs
    ) -> t.List["PathSpec"]:
        """
        Lower-level method that implements the actual request to list all
        possible geographic scope options.

        Given an API, its supported year, and an available (lower-level)
        geographic scope, return all compatible path specifications.

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

        kwargs
            Keyword arguments to supply for FIPS codes.
             
            Refer to the `fips` module to view corresponding FIPS codes
            and/or references to these geographic formatters.

        Returns
        -------
            A list of :py:class:`~PathSpec` tuples.
        """
        gs_d   = ShapeFileHandler._GEO_SCOPES_DICT
        fmtter = _FipsFMT._geo_fips_fmtter(**kwargs)

        _PathFMT._api_checker(api, year)
        API = _PathFMT._API_DATA[api][0]
        
        paths = _FipsFMT._compat_paths(API, year, geography)

        compat_paths = []
        for p in paths:
            fips_codes = [fmtter.get(r, '') for r in p.requires]
            fips_check = [i for i in fips_codes if i]

            ps = PathSpec(
                upper_scope  = p.upper,
                upper_level  = gs_d[p.upper][1],
                lower_scope  = p.lower,
                lower_level  = gs_d[p.lower][1],
                has_require  = len(p.requires) > 0,
                FIPS         = ''.join(fips_codes),
                is_full_spec = len(fips_codes) == len(fips_check)
            )
            compat_paths.append(ps)

        return compat_paths
    
    @classmethod
    def _compat_paths(
        cls, api: str, year: int, scope: str,
    ) -> t.List["GeoScopeComb"]:
        paths     = _PathFMT.geo_scope_combinations(api, year)
        cmp_paths = [p for p in paths if p.lower == scope]
        if len(cmp_paths) > 0:
            return cmp_paths
        else:
            raise GeoScopeError(
                f"'{scope}' is not available for the '{api}' API during {year}. "
                f"Available lower-level/child scopes include one of the following: "
                f"{list(set([p.lower for p in paths]))}."
            )
        
    @classmethod
    def _geo_fips_fmtter(cls, **kwargs: str):
        """
        Format and return keyword arguments with their respective
        FIPS codes.
        """
        _dd = _FipsFMT._FIPS_dict
        nkwargs = {}
        for k, v in kwargs.items():
            ins, cal = _dd[k]

            # Users have a choice between text or variables. If a user inputs text
            # we try and find it. Otherwise, it must be the variable referenced
            # from the `fips` module (an all-digit text). As a last ditch, we raise
            # a KeyError.
            try:
                nkwargs[k] = cal(**{k: v for k, v in kwargs.items() if k in ins})
            
            except TypeError as e:
                c_sign = inspect.signature(cal)
                missing = [p for p in c_sign.parameters if p not in kwargs.keys()]
                raise FIPSError(
                    f"Additional FIPS formatters are required: {missing}."
                ) from None
            
            except KeyError:
                if v.isdigit():
                    nkwargs[k] = v
                else:
                    raise FIPSError(
                        f"Could not locate an acceptable FIPS value for '{k}'. Reference "
                        "the `fips` module to view and/or reference acceptable FIPS codes."
                    ) from None

        return nkwargs


class _PathFMT:
    _BASE_URL: str = 'http://api.census.gov/data/{year}/{API}/geography.json'
    _API_DATA = api_data
    
    @classmethod
    def geo_scope_combinations(cls, api: str, year: int) -> t.List["GeoScopeComb"]:
        """
        Return all valid upper-level/lower-level geographic
        scope combinations.

        Parameters
        ----------
        api
            The ACS API of interest.

        year
            The year of interest.

            Note that the API be supported for this year.

        Returns
        -------
            A list of :py:class:`~GeoScopeComb` tuples.
        """
        paths = _PathFMT._avail_paths(api, year)
        gsc = []
        for child, pe in paths.items():
            for i in pe[3]:
                gsc.append(GeoScopeComb(i, child, paths[i].requires))

        return sorted(gsc, key = lambda gs: (paths[gs[0]], paths[gs[1]]))
    
    @classmethod
    def _avail_paths(cls, api: str, year: int) -> t.Dict[str, "PathEnum"]:
        """
        Return all possible :py:class:`~PathEnum` pathings for each
        lower-level scope supported by the API.

        Parameters
        ----------
        api
            The ACS API of interest.

        year
            The year of interest.

            Note that the API be supported for this year.

        Returns
        -------
            A dictionary whose keys are the lower-level scope and
            whose values are :py:class:`~PathEnum` pathings.
        """
        url   = _PathFMT._BASE_URL.format(year = year, API = api)
        gs_df = ShapeFileHandler._read_geo_scopes_df()
        gs_d  = ShapeFileHandler._GEO_SCOPES_DICT

        gs_j = req.get(url).json()

        paths_d: t.Dict[str, "PathEnum"] = {}
        for gs in gs_j["fips"]:
            if gs['geoLevelDisplay'] in [i[0] for i in gs_d.values()]:
                level    = gs['geoLevelDisplay']
                requires = gs.get('requires', [])

                k = [k for k, v in gs_d.items() if v[0] == level][0]
                requires = _FipsFMT._FIPS_dict[k][0]
                pathing = list(set([*requires, k]))

                parents = [k for k in gs_d.keys() if
                           not gs_df[(gs_df['Parent Summary Level'] == gs_d[k][0]) &
                                     (gs_df['Child Summary Level'] == level)].empty]

                paths_d[k] = PathEnum(level, requires, pathing, parents)

        return paths_d
    
    @classmethod
    def _api_checker(cls, api: str, year: int) -> None:
        _, years, _ = _PathFMT._API_DATA.get(api, (None, None, None))

        if not years:
            raise KeyError(
                f"'{api}' was not a recognizable American Community Survey API. "
                f"Supported APIs are one of the following: {list(_PathFMT._API_DATA.keys())}."
            )
        
        if year not in [year for year in years if year >= 2010]:
            raise ValueError(
                f"{year} is not a supported year for this API. Supported years for '{api}' "
                f"include one of the following: {[year for year in years if year >= 2010]}."
            )


PathSpec = namedtuple(
    'PathSpec',
    field_names = ('upper_scope', 'lower_scope',
                   'upper_level', 'lower_level',
                   'has_require', 'FIPS', 'is_full_spec')
)

PathEnum = namedtuple(
    'PathEnum',
    field_names = ('level', 'requires', 'pathing', 'parents'),
    defaults    = ('', [], [], [])
)

GeoScopeComb = namedtuple(
    'GeoScopeComb',
    field_names = ('upper', 'lower', 'requires'),
    defaults = ('', '', [])
)