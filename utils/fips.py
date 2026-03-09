"""
Handling FIPS codes, map documentation, and geographic scopes.

Executed once, but stored for posterity.
"""

from pathlib import Path
from datetime import datetime
import pandas as pd

from acspsuedo.source import (
    remove_accents,
    str_replacement
)

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
logger_fmt = logging.Formatter(
    fmt     = "%(asctime)s - %(filename)s, %(funcName)s function (Line %(lineno)s) - %(levelname)s: %(message)s",
    datefmt = "%m-%d-%Y, %I:%M:%S %p"
)
console_handler.setFormatter(logger_fmt)
logger.addHandler(console_handler)
logger.setLevel('INFO')


STR_REPL_DICT = {
    **{k: ''  for k in [".", "'", "?", "(", ")"]},
    **{k: '_' for k in ['-', "/", ' ']}
}




def fips_folder_init() -> None:
    """Initialize the FIPS folder."""
    FOLDER_PATH = 'fips'
    if not Path(FOLDER_PATH).exists():
        Path(FOLDER_PATH).mkdir(parents = True, exist_ok = True)
    
    FILE_PATH = 'fips/__init__.py'
    with open(FILE_PATH, 'w') as file:
        file.write(f'''"""

FOLDER LAST UPDATED: {datetime.now().strftime('%B %d, %Y')}                   

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
]'''
    )
    logger.info('Successfully initialized the fips folder: ./fips/')


def _states_df() -> pd.DataFrame:
    """Fetch the data for state FIPS codes."""
    URL = 'https://www2.census.gov/geo/docs/reference/codes2020/national_state2020.txt'
    STATES_df = pd.read_csv(
        URL,
        sep = '|',
        dtype = object
    )[['STATE', 'STATE_NAME', 'STATEFP']]

    STATES_df['STATE_NAME'] = STATES_df['STATE_NAME'].str.replace('.', '')
    return STATES_df


def states_fips() -> None:
    """Initialize the script containing state FIPS codes."""
    STATES_df = _states_df()

    FILE_PATH = 'fips/states.py'
    with open(FILE_PATH, 'w') as file:
        file.write(f'''"""

LAST UPDATED: {datetime.now().strftime('%B %d, %Y')}

FIPS codes for states (and their abbreviations).
                   
These FIPS codes are particularly useful for extracting
TIGER shapefiles that require a state geographic ID.

The URLs for these particular shapefiles are denoted
in the `func.geographies.ShapeFileHandler()` class with
the `state` string formatter.

"""\n\n\n\n'''
    )
        for _, row in STATES_df.iterrows():
            file.write(f"{(row['STATE_NAME'].upper().replace(' ', '_'))} = '{row['STATEFP']}'\n")
            file.write(f"{row['STATE']} = '{row['STATEFP']}'\n\n\n")

        file.write(f"STATE_FIPS = {{\n")
        for _, row in STATES_df.iterrows():
            file.write(f"    '{(row['STATE_NAME'].upper())}': '{row['STATEFP']}',\n")
        file.write("}\n\n\n")

        file.write(f"STATE_FIPS_ABBREV = {{\n")
        for _, row in STATES_df.iterrows():
            file.write(f"    '{row['STATE']}': '{row['STATEFP']}',\n")
        file.write("}\n\n\n")
    logger.info('Successfully wrote the script containing state FIPS codes: %s', FILE_PATH)


def zcta_by_state_fips() -> None:
    """Initialize the script containing zipcode tabulation area FIPS codes."""
    URL = 'https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt'
    zcta_df = pd.read_csv(URL, sep='|', dtype = object)
    zcta_df['STATEFP'] = [i[0:2] for i in zcta_df['GEOID_COUNTY_20']]
    zcta_df['ZCTA']    = zcta_df['GEOID_ZCTA5_20']

    states  = _states_df()
    zcta_df = zcta_df.merge(states[['STATEFP', 'STATE']], copy=True)

    ZCTA_df = zcta_df[['STATE', 'ZCTA']].dropna().sort_values(by = ['STATE', 'ZCTA'], ignore_index=True)
    ZCTA_df = ZCTA_df.drop_duplicates(ignore_index = True)

    FILE_PATH = 'fips/_zcta.py'
    with open(FILE_PATH, 'w') as file:
        file.write(f'''"""

LAST UPDATED: {datetime.now().strftime('%B %d, %Y')}

FIPS codes for states corresponding to the zipcode
tabulation areas (ZCTA) they contain.
                   
TIGER shapefiles for ZCTA geographic scopes require
a state geographic ID.

The URLs for these particular shapefiles are denoted
in the `func.geographies.ShapeFileHandler()` class with
the `state` string formatter.

Note that some ZCTAs may not be entirely circumscribed
by a state (e.g. ZCTAs may cross state lines).

"""\n\n\n\n'''
    )
        file.write("ZCTA = {\n")
        for STATE in sorted(ZCTA_df['STATE'].unique()):
            mask = ZCTA_df['STATE'] == STATE
            list_zcta = ZCTA_df['ZCTA'][mask]
            file.write(f"    '{STATE}': [\n")
            for zcta in list_zcta:
                file.write(f"        '{zcta}',\n")
            file.write(f"    ],\n")
        file.write("}\n\n\n")
    logger.info('Successfully wrote the script containing zipcode tabulation area FIPS codes: %s', FILE_PATH)


def counties_places_df() -> pd.DataFrame:
    URL = "https://www2.census.gov/geo/docs/reference/codes2020/national_place_by_county2020.txt"
    DF  = pd.read_csv(URL, sep='|', dtype = object)
    STATES_df = _states_df()
    DF = STATES_df.merge(DF, on = ['STATE', 'STATEFP'])
    DF['STATENAME'] = DF['STATE_NAME']
    DF = DF[['STATEFP', 'STATE', 'STATENAME', 'COUNTYFP', 'COUNTYNAME', 'PLACEFP', 'PLACENAME']]

    str_dict = {k: '' for k in
                [' borough', ' comunidad', ' town', ' CDP',
                 ' municipality', ' city', ' village', ' zona urbana']}

    DF['PLACENAME'] = [ str_replacement(i, str_dict) for i in DF['PLACENAME'] ]

    DF = DF.sort_values(['STATEFP', 'COUNTYFP', 'PLACEFP'], ignore_index = True)
    
    return DF


def places_by_state_fips() -> None:
    """Initialize the script containing place FIPS codes."""
    DF = counties_places_df()

    def append_counties_to_cities(
            city_series: pd.Series,
            county_series: pd.Series
    ) -> pd.Series:
        """
        If a place name has duplicate entries anywhere,
        append each entry's county name to any instance
        of the place name.
        """
        counts = city_series.value_counts()
        for item in counts.index:
            if counts[item] > 1:
                city_series[city_series == item] += ' ' + county_series[city_series == item]
        return city_series

    FILE_PATH = 'fips/_place.py'
    with open(FILE_PATH, 'w') as file:
        file.write(f'''"""

LAST UPDATED: {datetime.now().strftime('%B %d, %Y')}

FIPS codes for places, segmented by state.
                   
TIGER shapefiles for place geographic scopes require
a state geographic ID.

The URLs for these particular shapefiles are denoted
in the `func.geographies.ShapeFileHandler()` class with
the `state` string formatter.

Note that some place names may be non-unique and thus have
their respective county names appended.

"""\n\n\n\n'''
    )
        file.write("PLACE = {\n")
        state_abbvs = DF['STATE'].unique()
        for STATE in state_abbvs:
            state_df = DF[DF['STATE'] == STATE]
            state_df = state_df.copy() # <- Needed for when Pandas is updated to 3.0.0
            
            state_df['PLACENAME'] = append_counties_to_cities(state_df['PLACENAME'], state_df['COUNTYNAME'])
            state_df['PLACENAME'] = [str_replacement(i, STR_REPL_DICT) for i in state_df['PLACENAME']]

            file.write(f"    '{STATE}': {{\n")
            for PLACE_NAME, PLACE_FIPS in zip(state_df['PLACENAME'], state_df['PLACEFP']):
                file.write(f"""        "{remove_accents(PLACE_NAME).upper()}": '{PLACE_FIPS}',\n""")
            
            file.write("    },\n")
        file.write("}")
    logger.info('Successfully wrote the script containing place FIPS codes: %s', FILE_PATH)


def places_scripts_folder() -> None:
    """Initialize the module containing place variables segmented by state."""
    from acspsuedo.fips._place import PLACE
    DF = counties_places_df()

    FOLDER_PATH = 'fips/places'

    if not Path(FOLDER_PATH).exists():
        Path(FOLDER_PATH).mkdir(parents = True, exist_ok = True)
    
    FILE_PATH = f'{FOLDER_PATH}/__init__.py'
    with open(FILE_PATH, 'w') as file:
        file.write(f'''"""

FOLDER LAST UPDATED: {datetime.now().strftime('%B %d, %Y')}                   

FIPS codes for places, segmented by state.
                   
Note that some place names may be non-unique and thus have
their respective county names appended.

"""''')

    for state in PLACE.keys():
        STATE_NAME = DF['STATENAME'][DF['STATE'] == state].iloc[0]
        PLACE_DICT = PLACE[state]
        FILE_PATH = f'{FOLDER_PATH}/{state}.py'
        with open(FILE_PATH, 'w') as file:
            file.write(f'''"""                 
FIPS codes for places in the state/territory: {STATE_NAME}.
                
Note that some place names may be non-unique and thus have
their respective county names appended.
"""\n\n\n\n''')
            for variable, fips_code in PLACE_DICT.items():
                file.write(f"""{variable} = '{fips_code}'\n""")

    logger.info('Successfully wrote the module containing place FIPS codes: %s', FOLDER_PATH)
            


def counties_by_state_fips():
    """Initialize the script containing county FIPS codes."""
    DF = counties_places_df()
    DF = DF[['STATEFP', 'STATE', 'STATENAME', 'COUNTYFP', 'COUNTYNAME']]
    DF = DF.drop_duplicates(ignore_index = True)

    FILE_PATH = 'fips/_county.py'
    with open(FILE_PATH, 'w') as file:
        file.write(f'''"""

LAST UPDATED: {datetime.now().strftime('%B %d, %Y')}

FIPS codes for counties, segmented by state.
                   
TIGER shapefiles for county geographic scopes do NOT
require a state geographic ID since counties are
collected under the more general 'us' geographic
scope.

These are retained here specifically for handling
TIGER AREAWATER shapefiles, which are used to discard
water areas from geometries.

"""\n\n\n\n'''
    )
        file.write("COUNTY = {\n")
        state_abbvs = DF['STATE'].unique()
        for STATE in state_abbvs:
            state_df = DF[DF['STATE'] == STATE]

            file.write(f"    '{STATE}': {{\n")
            for COUNTY_NAME, COUNTY_FIPS in zip(state_df['COUNTYNAME'], state_df['COUNTYFP']):
                cleaned_COUNTY_NAME = str_replacement(remove_accents(COUNTY_NAME).upper(), STR_REPL_DICT)
                file.write(f"""        "{cleaned_COUNTY_NAME}": '{COUNTY_FIPS}',\n""")
            
            file.write("    },\n")
        file.write("}")
    logger.info('Successfully wrote the script containing county FIPS codes: %s', FILE_PATH)


def counties_scripts_folder() -> None:
    """Initialize the module containing county variables segmented by state."""
    from acspsuedo.fips._county import COUNTY
    DF = counties_places_df()

    FOLDER_PATH = 'fips/counties'

    if not Path(FOLDER_PATH).exists():
        Path(FOLDER_PATH).mkdir(parents = True, exist_ok = True)
    
    FILE_PATH = f'{FOLDER_PATH}/__init__.py'
    with open(FILE_PATH, 'w') as file:
        file.write(f'''"""

FOLDER LAST UPDATED: {datetime.now().strftime('%B %d, %Y')}                   

FIPS codes for counties, segmented by state.

"""''')

    for state in COUNTY.keys():
        STATE_NAME = DF['STATENAME'][DF['STATE'] == state].iloc[0]
        COUNTY_DICT = COUNTY[state]
        FILE_PATH = f'{FOLDER_PATH}/{state}.py'
        with open(FILE_PATH, 'w') as file:
            file.write(f'''"""                 
FIPS codes for counties in the state/territory: {STATE_NAME}.
"""\n\n\n\n''')
            for variable, fips_code in COUNTY_DICT.items():
                file.write(f"""{variable} = '{fips_code}'\n""")

    logger.info('Successfully wrote the module containing county FIPS codes: %s', FOLDER_PATH)


def main() -> None:    
    fips_folder_init()
    
    states_fips()
    
    zcta_by_state_fips()
    
    places_by_state_fips()
    places_scripts_folder()
    
    counties_by_state_fips()
    counties_scripts_folder()


if __name__ == '__main__':
    main()