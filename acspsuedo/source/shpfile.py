"""
Shapefile handler for user-defined geometries.

**NOTE**: We try our best to fit as many geometries as possible. However,
the Census Bureau has not maintained a uniform naming criteria throughout
the years for either the TIGER shapefile or Cartographic Boundary databases,
so requests for geometric information may not be satisfied.

If requests are not satisfied, we encourage users to peruse the map
documentation here: https://www2.census.gov/geo/tiger/.

See also the addendum that follows in this module for extra information.
"""

# ADDENDUM #1 (version 0.2.1)
# We add deterministic rules for some geographic components, but not
# for all. This is for two reasons:
# 1. The Bureau has not maintained a uniform naming criteria throughout
#    the years (e.g. 2012 bucks the predicted congressional district
#    session, and geometries for the 117th congressional district are not
#    avaiable because of the Census' previous methodological design that
#    steered away from collecting congressional district boundaries for
#    the session that aligns with the Decennial Census; cf.
#    https://www.census.gov/programs-surveys/geography/guidance/geo-areas/congressional-dist.html).
# 2. For some components, the Bureau has stored shapefiles in such a way
#    as to obfuscate a general URL locator rule. However, this is not
#    generally the case for more recent years which have become much more
#    standardized (post-2010 for TIGER shapefiles, or post-2014/15 Census
#    GeoDatabases, to be sure).
# It is for these reasons that a general rule is preferred.
#
# In any case, we implement a broad "net" (so to speak) and throw a warning
# for any stragglers and appropriately name these considerations when the
# user runs queries.

import os
import re
import shutil
import warnings
import typing as t
from pathlib import Path
from logging import getLogger

import geopandas as gpd
import requests

from acspsuedo.source.low.exceptions import APIException
from acspsuedo.source.shpfile_fmt import GEO_SPEC_METADATA
from acspsuedo.fips import STATE_FIPS


logger = getLogger(__name__)



class ShpfileException(APIException):
    """Custom exception class for TIGER Shapefile extraction-related errors."""
    pass

class ShpfileFormatterException(APIException):
    """Custom exception class for TIGER Shapefile formatting-related errors."""
    pass


class ShpfileWarning(UserWarning):
    """
    Custom warning class for TIGER Shapefile-related warnings.
    
    This will be used as per the module doc-string's guidance.
    """
    pass



drop_digits_re = re.compile(r"\d+")
"""
Regex pattern for dropping digits.
"""

drop_alphaletters_re = re.compile(r"[a-zA-Z]")
"""
Regex pattern for non-numeric, alphabetic characters.
"""


class ShpFileHandler:
    """
    Class for handling geometric extraction via TIGER shapefiles
    and/or Census GeoDatabases.
    """

    def __init__(
        self,
        auto_cache: bool = True,
        cache_path: t.Optional[t.Union[str, Path]] = None,
        track_updated_cache: bool = True
    ) -> None:
        """
        Initialization for :py:class:`acspsuedo.source.shpfile.ShpFileHandler`.

        Parameters
        ----------
        auto_cache
            Boolean; default True.
            
            Indicate whether or not to locally cache TIGER shapefiles.

        cache_path
            If `auto_cache` is True, indicate the caching folder in which TIGER
            shapefiles should be stored.
            
            Default `~/cache/acspsuedo/TIGER_shapefiles/`.

        track_updated_cache
            Boolean; default True.

            If the cache folder is updated, move all shapefiles from the previous
            cache folder to the updated cache folder.
        """

        self._auto_cache = auto_cache
        self._track_updated_cache = track_updated_cache
        
        if cache_path is None:
            cache_path = Path.home() / 'cache' / 'acspsuedo' / 'TIGER_shapefiles'
            self._cache_path = cache_path
        else:
            self._cache_path = Path(cache_path)

        # Internally track cached files
        self._cached_files = []

    
    @property
    def auto_cache(self):
        """
        Boolean indicating the state of locally caching fetched
        TIGER shapefiles. Default `True`.
        """
        return self._auto_cache
    
    @auto_cache.setter
    def auto_cache(self, new_state: bool):
        self._auto_cache = new_state

    @property
    def track_updated_cache(self):
        """
        Boolean indicating the whether or not we should move all files
        from the previous cache folder to the new cache folder. Default
        `True`.

        **NOTE**: With this implementation, it is assumed that the previous
        cached folder may be relevant beyond the immediate use within this
        overall architecture (i.e. to cache TIGER shapefiles that can be
        reached with this interface). For instance, users may want to cache
        TIGER shapefiles that they themselves have manually downloaded.
        
        Thus, we would be moving any of the existing cached files *without*
        deleting the previous cache folder.
        """
        return self._track_updated_cache
    
    @track_updated_cache.setter
    def track_updated_cache(self, new_state: bool):
        self._track_updated_cache = new_state

    @property
    def cache_path(self):
        """
        If `auto_cache` is True, this attribute specifies the local
        folder containing cached TIGER shapefiles.

        Default `~/cache/acspsuedo/TIGER_shapefiles/`.
        """
        return self._cache_path
    
    @cache_path.setter
    def cache_path(self, new_cache_path: t.Union[Path, str]):
        if self._track_updated_cache:
            self.__move_files_new_cache(new_cache_path)
        
        self._cache_path = Path(new_cache_path)

    @cache_path.deleter
    def cache_path(self):
        raise AttributeError("Cannot delete reference to the cache path.")
    


    def _tiger_url_fmtter_2008_2009(self, year: int, **geographic_specifiers: t.Any):
        for_scope, scope, _, outer = ShpFileHandler._tiger_init(year, **geographic_specifiers)
        base_url = f'https://www2.census.gov/geo/tiger/TIGER{year}/'

        # For special scopes, which require state identifiers
        if scope in ['BG', 'COUSUB', 'PLACE', 'TRACT', 'SLDL', 'SLDU', 'UNSD'] \
            or scope.startswith( ('CD', 'PUMA') ) \
            or outer == 'state':
            
            statefp = str(geographic_specifiers.get('state', ''))
            if not statefp:
                raise ShpfileFormatterException(
                    f"Geometries at the '{for_scope}' scope take a 'state' outer-level "
                    f"point of reference for extracting TIGER shapefile geometries at this "
                    f"scope. Missing a state FIPS code."
                )
            state = {v: k for k, v in STATE_FIPS.items()}.get(statefp, '')
            
            base_url += f'{statefp}_{state.replace(' ', '_')}/'
            path = f'tl_{year}_{statefp}_{scope.lower()}'
            
            if not scope.startswith( ('CD', 'PUMA') ):
                path += '00'
            
            path += '.zip'
        
        else:
            # Hmm...
            path = f'tl_{year}_us_{scope.lower()}.zip'

        return base_url, path


    def _tiger_url_fmtter_pre_2008_and_2010(self, year: int, **geographic_specifiers: t.Any):
        _, scope, folder, outer = ShpFileHandler._tiger_init(year, **geographic_specifiers)

        if outer == 'state':
            outer = geographic_specifiers.get('state', '')

        nested = year
        if scope.startswith( ('CD',) ):
            nested = drop_alphaletters_re.sub('', scope)

        suffix = str(year)[-2:]
        if scope.startswith( ('CD', 'PUMA', 'ZCTA', 'UAC') ):
            suffix = ''
        
        base_url = f'https://www2.census.gov/geo/tiger/TIGER2010/{folder}/{nested}/'
        path = f'tl_2010_{outer}_{scope.lower()}{suffix}.zip'

        return base_url, path


    def _tiger_url_fmtter_post_2010(self, year: int, **geographic_specifiers: t.Any):
        for_scope, scope, folder, outer = ShpFileHandler._tiger_init(year, **geographic_specifiers)

        if outer == 'state':
            outer = geographic_specifiers.get('state', '')
            if not outer:
                raise ShpfileFormatterException(
                    f"Geometries at the '{for_scope}' scope take a 'state' outer-level "
                    f"point of reference for extracting TIGER shapefile geometries at this "
                    f"scope. Missing a state FIPS code."
                )

        base_url = f'https://www2.census.gov/geo/tiger/TIGER{year}/{folder}/'
        path = f'tl_{year}_{outer}_{scope.lower()}.zip'

        return base_url, path

    def _tiger_url_fmtter(self, year: int, **geographic_specifiers) -> t.Tuple[str, str]:
        if 2008 <= year <= 2009:
            return self._tiger_url_fmtter_2008_2009(year, **geographic_specifiers)
        elif year == 2010 or year < 2008:
            return self._tiger_url_fmtter_pre_2008_and_2010(year, **geographic_specifiers)
        else:
            return self._tiger_url_fmtter_post_2010(year, **geographic_specifiers)


    def fetch_tiger_shpfile(self, year, **geographic_specifiers) -> t.Optional[gpd.GeoDataFrame]:
        """
        Fetch the appropriate TIGER shapefile for the supplied geographic specifiers
        and return a formated :py:class:`geopandas.GeoDataFrame` containing identifier
        and geometric information.

        **NOTE**: We try our best to fit as many geometries as possible. However,
        the Census Bureau has not maintained a uniform naming criteria throughout
        the years for either the TIGER shapefile or Cartographic Boundary databases,
        so requests for geometric information may not be satisfied.

        If requests are not satisfied, we encourage users to peruse the map
        documentation here: https://www2.census.gov/geo/tiger/.

        Returns
        -------
        If the appropriate TIGER shapefile is found, returns a :py:class:`geopandas.GeoDataFrame`
        instance.
        """

        if (gdf := self._fetch_tiger_shpfile(year, **geographic_specifiers)) is None:
            return
        
        return gdf

        
    def _fetch_tiger_shpfile(self, year: int, **geographic_specifiers: t.Any) -> t.Optional[gpd.GeoDataFrame]:
        """
        The actual underlying for running an attempt to the TIGER shapefile,
        as suggested by our approximation of the map data naming convention.
        """

        gdf = self._cache_fetch_tiger_shpfile(year, **geographic_specifiers)
        
        if gdf is None:
            return
        
        gdf = self._tiger_shpfile_fmtter(gdf, year, **geographic_specifiers)
        return gdf
    
    def _tiger_shpfile_fmtter(self, gdf: gpd.GeoDataFrame, year: int, **geographic_specifiers) -> gpd.GeoDataFrame:
        """
        Formatter for fetched TIGER shapefiles.
        """
        for_scope, _, _, _ = ShpFileHandler._tiger_init(year, **geographic_specifiers)

        # Create a year column
        gdf['YEAR'] = year

        # Keep identifier and geometric info
        gdf.columns = [drop_digits_re.sub('', col) for col in gdf.columns]

        id_cols = GEO_SPEC_METADATA[for_scope][3]
        if not isinstance(id_cols, list):
            id_cols = [id_cols]

        id_cols = [col for col in [*id_cols, 'GEOID', 'YEAR'] if col in list(gdf.columns)]

        gdf_cols = [*id_cols, 'geometry']
        gdf = gdf[gdf_cols].copy()

        # Rename GEOID to cohere with the GEO_ID in the Census queried data
        gdf.rename(columns = {'GEOID': 'GEO_ID'}, inplace = True)

        # Sort by
        if 'GEO_ID' in gdf.columns:
            gdf.sort_values(by = 'GEO_ID', ignore_index=True, inplace=True)
        else:
            gdf.sort_values(by = id_cols, ignore_index=True, inplace = True)

        return gdf
    
    def _cache_fetch_tiger_shpfile(self, year: int, **geographic_specifiers):
        """
        The actual underlying for fetching TIGER shapefiles and (if specified)
        caching them locally within the specified cache folder.
        """
        base_url, path = self._tiger_url_fmtter(year, **geographic_specifiers)

        try:
            # Check if caching is enabled. Then, check if the shapefile has already been cached.
            if self._auto_cache:
                file_path = f"{self._cache_path}/{path.removesuffix('.zip')}.shp"
                if Path(file_path).exists():
                    gdf = gpd.read_file(file_path)
                    return gdf
            
            content = _fetch_shpfile(f'{base_url}{path}')
            gdf = gpd.read_file(content)
            
            if self._auto_cache:
                self._cache_init()
                gdf.to_file(
                    filename = file_path,
                    index    = False
                )
                self._cached_files.append(path.removesuffix('.zip'))
            
            return gdf
        
        except ShpfileException:
            for_scope, _, _, _ = ShpFileHandler._tiger_init(year, **geographic_specifiers)
            msg = \
            f"\nCould not extract the appropriate TIGER shapefile for the '{for_scope}' scope \n" \
            f"during the {year} calendar year. This is partially due to several reasons: \n" \
            "   1. The Census Bureau has not maintained a uniform naming convention for its map file\n" \
            "      throughout the years.\n"\
            "   2. While attempts have been made to implement custom deterministic rules for generating \n"\
            "      the appropriate URLs for each geographic scope, this may not be successful in virtue \n"\
            "      of the previously stated reason and the fact that some files are stored in a different \n"\
            "      folders across the years, or aren't made available at all. This is particularly the case \n"\
            "      for data years 2008 and 2009.\n"\
            "See `acspsuedo.shpfile` for more information."\

            warnings.warn(
                msg,
                ShpfileWarning
            )
            return 
    
    def _cache_init(self):
        """
        Initialization for caching. Only applies if `auto_cache` is True.
        """
        if self._auto_cache:
            self._cache_path.mkdir(parents = True, exist_ok = True)

    def __move_files_new_cache(self, new_cache: t.Union[str, Path]):
        """Internal for moving new files to the updated cache."""
        file_dict = {}
        for root, _, files in os.walk(self._cache_path):
            for file in files:
                if any(path in file for path in self._cached_files):
                    file_dict[f'{root}/{file}'] = f'{new_cache}/{file}'
        
        for old_path, new_path in file_dict.items():
            shutil.move(old_path, new_path)

    @classmethod
    def _tiger_init(cls, year: int, **geographic_specifiers):
        """
        Initialization for TIGER shapefile configuration.
        
        Used to format case-match oddities in URL destinations.
        """
        for_scope, scope, folder = cls.__tiger_scope(year, **geographic_specifiers)
        outer = cls.__needs_scope(year, **geographic_specifiers)

        return for_scope, scope, folder, outer
    
    @classmethod
    def __tiger_scope(cls, year: int, **geographic_specifiers: t.Any) -> t.Tuple[str, str, str]:
        """
        Get the TIGER scope based off of the indicated `for` clause.
        For special scopes, we apply any custom deterministic rules
        via case-matching.
        """
        for_scope = list(geographic_specifiers)[-1]
        shp_scope = GEO_SPEC_METADATA[for_scope][2]
        shp_scope = shp_scope if shp_scope is not None else ''

        folder = shp_scope

        match shp_scope:
            case 'CD':
                shp_scope += str(_congressional_district_rule(year))
            case 'ZCTA':
                shp_scope, folder = _zipcode_tabulation_area_rule(year)
            case 'PUMA':
                shp_scope, folder = _public_use_microdata_area_rule(year)
            case 'SUBMCD':
                shp_scope, folder = _subminor_civil_division_rule(year)
            case 'UAC':
                shp_scope, folder = _urban_area_rule(year)
        
        return for_scope, shp_scope, folder
    
    @classmethod
    def __needs_scope(cls, year: int, **geographic_specifiers: t.Any) -> t.Optional[str]:
        """
        For some TIGER scopes, a state specifier must be specified to
        enforce scopes being taken from the outer-reference point of
        state-level as opposed to one at the nation-level.
        """
        for_scope = list(geographic_specifiers)[-1]
        shpfile_scope = GEO_SPEC_METADATA[for_scope][2]
        outer = GEO_SPEC_METADATA[for_scope][4]

        # For some years, congressional districts only had a 'us' outer reference.
        # But after 2022, they reverted to the 'state' outer reference...
        if shpfile_scope == 'CD' and year >= 2022:
            outer = 'state'
        
        return outer



shapefile_handler: ShpFileHandler = ShpFileHandler()



# ------------ Rules for special scopes ------------ #
# These deterministic rules are implemented here for
# special scopes from the TIGER shapefile data. Under,
# usual circumstances, most scopes will adhere to a
# general implementation and only change marginally,
# depending on whether the outer point of reference
# is the nation-level or state-level, but the
# following scopes must be manually handled. There
# will be a handful of extringent circumstances that
# these custom rules fail to handle, but the
# catch-all warning should suffice for the time being.
# ------------ -----------  ----------- ------------ #

def _congressional_district_rule(year: int):
    """
    Bureau's guidance on congressional districting:
    https://www.census.gov/programs-surveys/geography/guidance/geo-areas/congressional-dist.html

    Note that these congressional sessions specify align with those suggested
    by Census Bureau data and may/may not cohere with the actual session years.
    """

    # Congressional districts by sessions
    # - 103th (1993, 1994)
    # - 104th (1995, 1996)
    # - 105th (1997, 1998)
    # - 106th (1999, 2000)
    # - 107th (2001, 2002)
    # - 108th (2003, 2004)
    # - 109th (2005, 2006)
    # - 110th (2007, 2008)
    # - 111th (2009, 2010)
    # - 112th (2011, 2012)
    # - 113th (2013)
    # - 114th (2014, 2015)
    # - 115th (2016, 2017)
    # - 116th (2018, 2019, 2020, 2021)
    # - 118th (2022, 2023)
    # - 119th (2024)

    # For CD sessions 103 to 110 (i.e. 1993 to 2008):
    # https://www2.census.gov/geo/tiger/PREVGENZ/cd/
    # Note that each starts with state FIPS, e.g.
    # cd06_103_ship.zip for CA congressional districts

    cd_session = 103 + (year - 1993) // 2

    # Special years that disobey the general rule. Pandemic?
    if 2020 <= year <= 2021:
        return 116
    
    # There is a weird regime switch that occurs at 2013-14. From preliminary
    # research (cf. congressional district documentation in docstring above),
    # it may have seemed justified for the Bureau to remap congressional
    # districts given House seats were apportioned in the 113th session based
    # on the 2010 Decennial Census.
    if year >= 2014:
        return 114 + (year - 2014) // 2
    
    return cd_session


def _zipcode_tabulation_area_rule(year: int):
    zcta = f'ZCTA5{str(year)[2]}0'
    folder = f'ZCTA5{str(year)[2]}0'

    if 2010 <= year <= 2019:
        folder = 'ZCTA5'

    return zcta, folder

def _public_use_microdata_area_rule(year: int):
    folder = f'PUMA{str(year)[2]}0'
    if year == 2010:
        folder = 'PUMA5'
    if 2011 <= year <= 2023:
        folder = 'PUMA'

    puma = f'PUMA{str(year)[2]}0'
    if 2008 <= year <= 2009:
        puma = 'PUMA500'
    if 2020 <= year <= 2021:
        puma = 'PUMA10'

    return puma, folder

def _subminor_civil_division_rule(year: int):
    """
    This only applies for Puerto Rico, since they have subbarrios
    and not subminor civil divisions (semantically different).

    *Note*: This applies for 2010 and later. For 2008 and 2009, we
    simply point back to the naming convention error because subbarrios
    are nested in their county folders as opposed to the state folder
    for these particular years
    """
    folder = 'SUBMCD'
    scd = 'SUBMCD'
    
    if year >= 2013:
        scd = 'SUBBARRIO'
    if year >= 2020:
        folder = 'SUBBARRIO'

    return scd, folder

def _urban_area_rule(year: int):
    folder = f'UAC{str(year)[2]}0'
    if year == 2010:
        folder = 'UA'
    if 2011 <= year <= 2023:
        folder = 'UAC'

    uac = f'UAC{str(year)[2]}0'
    
    if 2020 <= year <= 2021:
        uac = 'UAC10'

    return uac, folder




def _fetch_shpfile(url: str) -> t.Optional[bytes]:
    """
    Synchronous method to fetch the binary content of a url.

    This is the underlying for fetching TIGER shapefile data.
    """
    resp = requests.get(url)

    if (status := resp.status_code) == 404:
        raise ShpfileException(
            f"Could not fetch the binary content of '{url}'. HTTPS Status Code: {status}."
        )
    
    if (content_type := resp.headers['Content-Type']) != 'application/zip':
        raise APIException(
            f"Expected 'application/zip' content-type. Received '{content_type}'."
        )

    return resp.content