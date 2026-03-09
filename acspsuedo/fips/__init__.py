"""

FOLDER LAST UPDATED: February 22, 2026                   

Federal Information Processing Series (FIPS)
codes for geographic scopes.
                   
FIPS codes are used to format TIGER shapefiles
and query American Community Survey (ACS) data
with the Census Bureau's API.

"""

from acspsuedo.fips._zcta import ZCTA
from acspsuedo.fips._place import PLACE
from acspsuedo.fips._county import COUNTY
from acspsuedo.fips.states import STATE_FIPS, STATE_FIPS_ABBREV

__all__ = [
    "COUNTY",
    "PLACE",
    "ZCTA",
    "STATE_FIPS",
    "STATE_FIPS_ABBREV"
]