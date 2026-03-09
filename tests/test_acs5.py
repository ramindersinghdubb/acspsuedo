import acspsuedo.data as apd

from acspsuedo.api import ACS5
from acspsuedo.fips.counties.CA import LOS_ANGELES_COUNTY
from acspsuedo.fips.states import CA

import pandas as pd
import geopandas as gpd



apd.CONFIG.is_cache = False


df = apd.download_by_geo_collection(
    ACS5,
    2020,
    'B25058',
    'place',
    state = CA,
    county = LOS_ANGELES_COUNTY
)

gdf = apd.download_by_geo_collection(
    ACS5,
    2020,
    'B25058',
    'place',
    include_geometries = True,
    state = CA,
    county = LOS_ANGELES_COUNTY
)



assert isinstance(df, pd.DataFrame)
assert isinstance(gdf, gpd.GeoDataFrame)
assert(len(df) == len(gdf))