"""
Handling FIPS codes, map documentation, and geographic scopes.

Executed once, but stored for posterity.
"""

from pathlib import Path
from datetime import datetime
import logging

import pandas as pd

from acspsuedo.source.low.callables import (
    remove_accents,
    str_replacement
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
logger_fmt = logging.Formatter(
    fmt     = "%(asctime)s - %(filename)s, %(funcName)s function (Line %(lineno)s) - %(levelname)s: %(message)s",
    datefmt = "%m-%d-%Y, %I:%M:%S %p"
)
console_handler.setFormatter(logger_fmt)
logger.addHandler(console_handler)


STR_REPL_DICT = {
    **{k: ''  for k in [".", "'", "?", "(", ")"]},
    **{k: '_' for k in ['-', "/", ' ']}
}




def fips_folder_init() -> None:
    """Initialize the FIPS folder."""
    FOLDER_PATH = 'acspsuedo/fips'
    if not Path(FOLDER_PATH).exists():
        Path(FOLDER_PATH).mkdir(parents = True, exist_ok = True)
    
    FILE_PATH = 'acspsuedo/fips/__init__.py'
    with open(FILE_PATH, 'w') as file:
        file.write(f'''"""
FOLDER LAST UPDATED: {datetime.now().strftime('%B %d, %Y')}                   

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

    FILE_PATH = 'acspsuedo/fips/states.py'
    with open(FILE_PATH, 'w') as file:
        file.write(f'''"""
LAST UPDATED: {datetime.now().strftime('%B %d, %Y')}

FIPS codes for states (and their abbreviations).
                   
These FIPS codes are particularly useful for extracting
TIGER shapefiles that require a state geographic ID.
"""\n\n\n\n'''
    )
        for _, row in STATES_df.iterrows():
            file.write(f"{(row['STATE_NAME'].upper().replace(' ', '_'))} = '{row['STATEFP']}'\n")
            file.write(f"{row['STATE']} = '{row['STATEFP']}'\n\n\n")

        file.write(f"STATE_FIPS = {{\n")
        for _, row in STATES_df.iterrows():
            file.write(f"    '{(row['STATE_NAME'].upper())}': {row['STATE_NAME'].upper().replace(' ', '_')},\n")
        file.write("}\n\n\n")

        file.write(f"ABBREV_STATE_FIPS = {{\n")
        for _, row in STATES_df.iterrows():
            file.write(f"    '{row['STATE']}': {row['STATE']},\n")
        file.write("}\n\n\n")
    logger.info('Successfully wrote the script containing state FIPS codes: %s', FILE_PATH)


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

    FILE_PATH = 'acspsuedo/fips/_place.py'
    with open(FILE_PATH, 'w') as file:
        file.write(f'''"""
LAST UPDATED: {datetime.now().strftime('%B %d, %Y')}

FIPS codes for places, segmented by state.
                   
TIGER shapefiles for place geographic scopes require
a state geographic ID.

Note that some place names may be non-unique and thus have
their respective county names appended.
"""\n\n\n\n'''
    )
        file.write("PLACE_BY_STATE = {\n")
        state_abbvs = DF['STATE'].unique()
        for STATE in state_abbvs:
            state_df = DF[DF['STATE'] == STATE]
            state_df = state_df.copy() # <- Needed for when Pandas is updated to 3.0.0
            
            state_df['PLACENAME'] = append_counties_to_cities(state_df['PLACENAME'], state_df['COUNTYNAME'])
            state_df['PLACENAME'] = [str_replacement(i, STR_REPL_DICT) for i in state_df['PLACENAME']]

            file.write(f"    '{STATE}': {{\n")
            for PLACE_NAME, PLACE_FIPS in zip(state_df['PLACENAME'], state_df['PLACEFP']):
                file.write(f"""        "{remove_accents(PLACE_NAME).title()}": '{PLACE_FIPS}',\n""")
            
            file.write("    },\n")
        file.write("}")
    logger.info('Successfully wrote the script containing place FIPS codes: %s', FILE_PATH)


def places_scripts_folder() -> None:
    """Initialize the module containing place variables segmented by state."""
    from acspsuedo.fips._place import PLACE_BY_STATE
    DF = counties_places_df()

    FOLDER_PATH = 'acspsuedo/fips/places'

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

    for state in PLACE_BY_STATE:
        STATE_NAME = DF['STATENAME'][DF['STATE'] == state].iloc[0]
        PLACE_DICT = PLACE_BY_STATE[state]
        FILE_PATH = f'{FOLDER_PATH}/{state}.py'
        with open(FILE_PATH, 'w') as file:
            file.write(f'''"""                 
FIPS codes for places in the state/territory: {STATE_NAME}.
                
Note that some place names may be non-unique and thus have
their respective county names appended.
"""\n\n\n\n''')
            for variable, fips_code in PLACE_DICT.items():
                file.write(f"""{variable.title()} = '{fips_code}'\n""")

    logger.info('Successfully wrote the module containing place FIPS codes: %s', FOLDER_PATH)
            


def counties_by_state_fips():
    """Initialize the script containing county FIPS codes."""
    DF = counties_places_df()
    DF = DF[['STATEFP', 'STATE', 'STATENAME', 'COUNTYFP', 'COUNTYNAME']]
    DF = DF.drop_duplicates(ignore_index = True)

    FILE_PATH = 'acspsuedo/fips/_county.py'
    with open(FILE_PATH, 'w') as file:
        file.write(f'''"""
LAST UPDATED: {datetime.now().strftime('%B %d, %Y')}

FIPS codes for counties, segmented by state.
"""\n\n\n\n'''
    )
        file.write("COUNTY_BY_STATE = {\n")
        state_abbvs = DF['STATE'].unique()
        for STATE in state_abbvs:
            state_df = DF[DF['STATE'] == STATE]

            file.write(f"    '{STATE}': {{\n")
            for COUNTY_NAME, COUNTY_FIPS in zip(state_df['COUNTYNAME'], state_df['COUNTYFP']):
                cleaned_COUNTY_NAME = str_replacement(remove_accents(COUNTY_NAME).title(), STR_REPL_DICT)
                file.write(f"""        "{cleaned_COUNTY_NAME}": '{COUNTY_FIPS}',\n""")
            
            file.write("    },\n")
        file.write("}")
    logger.info('Successfully wrote the script containing county FIPS codes: %s', FILE_PATH)


def counties_scripts_folder() -> None:
    """Initialize the module containing county variables segmented by state."""
    from acspsuedo.fips._county import COUNTY_BY_STATE
    DF = counties_places_df()

    FOLDER_PATH = 'acspsuedo/fips/counties'

    if not Path(FOLDER_PATH).exists():
        Path(FOLDER_PATH).mkdir(parents = True, exist_ok = True)
    
    FILE_PATH = f'{FOLDER_PATH}/__init__.py'
    with open(FILE_PATH, 'w') as file:
        file.write(f'''"""
FOLDER LAST UPDATED: {datetime.now().strftime('%B %d, %Y')}                   

FIPS codes for counties, segmented by state.
"""''')

    for state in COUNTY_BY_STATE:
        STATE_NAME = DF['STATENAME'][DF['STATE'] == state].iloc[0]
        COUNTY_DICT = COUNTY_BY_STATE[state]
        FILE_PATH = f'{FOLDER_PATH}/{state}.py'
        with open(FILE_PATH, 'w') as file:
            file.write(f'''"""                 
FIPS codes for counties in the state/territory: {STATE_NAME}.
"""\n\n\n\n''')
            for variable, fips_code in COUNTY_DICT.items():
                file.write(f"""{variable.title()} = '{fips_code}'\n""")

    logger.info('Successfully wrote the module containing county FIPS codes: %s', FOLDER_PATH)


def main() -> None:    
    fips_folder_init()
    
    states_fips()
    
    places_by_state_fips()
    counties_by_state_fips()

    places_scripts_folder()
    counties_scripts_folder()


if __name__ == '__main__':
    main()