"""
Functions for updating available American Community Survey APIs
and metadata on American Community Survey datasets.

Executed daily.
"""

import requests as req
import pandas as pd
from datetime import datetime

from acspsuedo.geographies import ShapeFileHandler
from acspsuedo.source import str_replacement, batch_GET


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



supported_scopes = ShapeFileHandler._GEO_SCOPES_DICT



def _api_df_fmt(list_api: list[dict]) -> pd.DataFrame:
    """
    Initialize the download of Census Bureau's
    American Community Survey APIs (if not already
    done).
    """
    ACS_API_df = pd.DataFrame(
        [{'YEAR': ACS_API.get('c_vintage'),
          'BASE': '/'.join( [i for i in ACS_API.get('c_dataset', '')] ),
          'BASE_URL': ACS_API.get('distribution', '')[0].get('accessURL'),
          'GEOGRAPHIES_URL': ACS_API.get('c_geographyLink'),
          'GROUPS_URL': ACS_API.get('c_groupsLink'),
          'VARIABLES_URL': ACS_API.get('c_variablesLink'),
          'API_NAME': ACS_API.get('title')
        } for ACS_API in list_api ])
    
    geo_scopes = []
    for resp in batch_GET(list(ACS_API_df['GEOGRAPHIES_URL'])):
        geo_dict = resp['fips']
        geo_scopes.append(
            [ j for j in supported_scopes if any(supported_scopes[j][0] == i['geoLevelDisplay'] for i in geo_dict)]
        )
    ACS_API_df['SUPPORTED_GEO_SCOPES'] = geo_scopes

    REFS = []
    for BASE in ACS_API_df['BASE']:
        REF = str_replacement(BASE.upper(), {'/': '_', 'ACS_': ''})
        REFS.append(REF)

    ACS_API_df['REF'] = REFS

    DF = ACS_API_df.sort_values(by = ['YEAR', 'BASE'], ignore_index = True)

    return DF


def create_acs_api_dataset() -> pd.DataFrame:
    """
    Download the Census Bureau's American Community Survey
    APIs into a formatted dataset.
    """
    URL = 'https://api.census.gov/data/'
    logger.info("Running request to '%s'...", URL)
    response = req.get(URL)

    if response.status_code == 200:
        logger.info("Success! Cleaning API information...")

        CENSUS_DATA_DICT = response.json()

        LIST_ACS_API = [i for i in CENSUS_DATA_DICT['dataset']
                        if '/'.join(i.get('c_dataset')).startswith(('acs/acs'))
                        ]

        DF = _api_df_fmt(LIST_ACS_API)

        logger.info("Successfully formatted ACS API dataframe.")
        
        return DF
    else:
        logger.error("Error. Could not retrieve API info. HTTPS Status Code: %s", response.status_code)
        response.raise_for_status()


def write_api_metadata() -> None:
    """Write the actual API variable reference script."""
    
    df = create_acs_api_dataset()

    FILE_PATH = 'acspsuedo/api.py'
    with open(FILE_PATH, 'w') as file:
        file.write(f'''"""

LAST UPDATED: {datetime.now().strftime('%B %d, %Y')}

Structured API information for each of the American
Community Survey's APIs.

{{Reference: (Base name, available years, supported scopes)}}

Contains information on upper-/lower-level geographic scopes
as well.

"""\n
import typing as t\n\n\n\n''')
        for REF in sorted(df['REF'].unique()):
            file.write(f"{REF} = '{REF}'\n\n")
        file.write('\n\n\n')
        file.write(f"API_DATA: t.Dict[\n")
        file.write(f"    str, t.Tuple[str,\n")
        file.write(f"                 t.List[int],\n")
        file.write(f"                 t.List[str]\n")
        file.write(f"                ]\n")
        file.write(f"] = {{\n")
        for BASE in sorted(df['BASE'].unique()):
            mask = df['BASE'] == BASE
            
            REF = df['REF'][mask].iloc[0]
            YEARS = list(df[mask]['YEAR'])
            SCOPES = df[mask]['SUPPORTED_GEO_SCOPES'].iloc[-1]
            file.write(f"    '{REF}': (\n")
            file.write(f"        '{BASE}',\n")
            file.write(f"        {YEARS},\n")
            file.write(f"        {SCOPES}\n")
            file.write(f"    ),\n\n")
        file.write(f"}}\n\n\n\n")
        file.write('''"""
Parent/child-level geographic scopes.

{ Geographic scope: (parent-level, child-level, gdf columns,
shapefile level formatter(s), (optional) shapefile url
constructor) }

When shapefiles are queried, it will be on the child-level
scopes. These queried shapefiles, in turn, are merged onto
the fetched Census Bureau data.

Note that the specified gdf columns are additional columns
on which to add to `['GEOID', 'INTPLAN', 'INTPLOT', 'geometry']`.
This provides additional info on geocoding up and above `'GEOID'`.
Note as well that `['INTPLAN', 'INTPLOT']` are added to assist
plotting `plotly.express` tile-based map traces.

There will almost always be coherence between parent and child
levels owing to a lack of (implemented) geographic variants.
Variants exist for a select few classes and are updated per the
Census Bureau's guidance:
https://www.census.gov/programs-surveys/geography/guidance/geo-variants.html.
This may be subject to change in future developments.
"""

def _zcta_shpfile_spec(year: int):
    # Year is returned in this way because the 2011 TIGER files do not have ZCTA files
    # cf. https://www2.census.gov/geo/tiger/TIGER2011/
    subfolder, year = ('ZCTA5/2010', 2010) if year <= 2019 else (f'ZCTA5{str(year)[2]}0', year)
    scope = f'zcta5{str(year)[2]}0'
    return year, subfolder, scope

def _block_shpfile_spec(year: int):
    subfolder = 'TABBLOCK/2010' if year <= 2019 else f'TABBLOCK{str(year)[2]}0'
    scope = f'tabblock{str(year)[2]}0'
    return year, subfolder, scope

_GEO_PARENT_CHILD_DICT: t.Dict[
    str, t.Tuple[t.Optional[str],
                 t.Optional[str],
                 t.Optional[t.Union[str, list[str]]],
                 t.Optional[str],
                 t.Optional[t.Callable[..., t.Tuple]]]
] = {
    'tract': (
        '140',
        '1400000',
        ['STATEFP', 'COUNTYFP', 'TRACTCE'],
        '{state}',
        None
    ),
    'place': (
        '160',
        '1600000',
        ['STATEFP', 'PLACEFP'],
        '{state}',
        None
    ),
    'county': (
        '050',
        '0500000',
        ['STATEFP', 'COUNTYFP'],
        'us',
        None
    ),
    'state': (
        '040',
        '0400000',
        'STATEFP',
        'us',
        None
    ),
    'us': (
        '010',
        '0100000',
        None,
        None,
        None
    ),
    # These can only be accessed at the child-level scopes
    # (i.e. no parent-level scopes)
    'block': (
        None,
        '1500000',
        ['STATEFP', 'COUNTYFP', 'TRACTCE', 'BLOCKCE'],
        '{state}',
        lambda year: _zcta_shpfile_spec(year),
    ),
    'zcta': (
        '860',
        '8600000',
        'ZCTACE',
        'us',
        lambda year: _block_shpfile_spec(year)
    ),
}
''')

    logger.info("Successfully wrote the ACS API script: '~/acspsuedo/api.py'")




def main() -> None:
    write_api_metadata()


if __name__ == '__main__':
    main()
