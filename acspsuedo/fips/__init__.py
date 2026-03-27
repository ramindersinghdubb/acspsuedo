"""
FOLDER LAST UPDATED: March 20, 2026                   

Federal Information Processing Series (FIPS) codes for
geographic scopes.
                   
FIPS codes are used to format TIGER shapefiles and query
American Community Survey datasets using the Census Bureau's
API.
"""

from acspsuedo.fips._place import PLACE_BY_STATE
from acspsuedo.fips._county import COUNTY_BY_STATE
from acspsuedo.fips.states import STATE_FIPS, ABBREV_STATE_FIPS

__all__ = [
    "COUNTY_BY_STATE",
    "PLACE_BY_STATE",
    "STATE_FIPS",
    "ABBREV_STATE_FIPS"
]