"""
FOLDER LAST UPDATED: April 05, 2026                   

Federal Information Processing Series (FIPS) codes for
geographic scopes.
                   
FIPS codes are used to format TIGER shapefiles and query
American Community Survey datasets using the Census Bureau's
API.

Note:

- `acspsuedo.fips.states`

    FIPS codes for all 50 states, the Commonwealth of Puerto Rico, the
    Commonwealth of the Northern Mariana Islands, Guam, American Samoa,
    the Virgin Islands, and Minor Outlying Islands.

- `acspsuedo.fips.aiaanh`

    FIPS codes for American Indian/Alaska Native/Native Hawaiian Area
    (AIANNH) geographies across all states and territories of the United
    States.

- `acspsuedo.fips.counties`

    FIPS codes for counties across all states and territories of the United
    States. Note that FIPS codes for counties are segemented by state, e.g.
    `acspsuedo.fips.counties.new_york` contains county-level codes for the
    state of New York.

- `acspsuedo.fips.cousub`

    FIPS codes for county subdivisions across all states and territories of
    the United States. Note that FIPS codes for counties are segemented by
    states, e.g. `acspsuedo.fips.cousub.new_york` contains county
    subdivision-level codes for the state of New York.

- `acspsuedo.fips.places`

    FIPS codes for places/cities across all states and territories of the
    United States. Note that FIPS codes for places are segemented by state,
    e.g. `acspsuedo.fips.places.new_york` contains place-level codes for
    the state of New York.
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