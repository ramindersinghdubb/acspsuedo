"""
Functions for updating available American Community Survey APIs
and metadata on American Community Survey datasets.

Executed daily.
"""
import os
from datetime import datetime
import logging

import pandas as pd

from acspsuedo.source.low.protocols import fetch_content


logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
logger_fmt = logging.Formatter(
    fmt     = "%(asctime)s - %(filename)s, %(funcName)s function (Line %(lineno)s): %(levelname)s - %(message)s",
    datefmt = "%m-%d-%Y, %I:%M:%S %p"
)
console_handler.setFormatter(logger_fmt)
logger.addHandler(console_handler)

logger.setLevel("INFO")




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

    REFS = []
    for BASE in ACS_API_df['BASE']:
        REF = BASE.upper().replace('/', '_').replace('ACS_', '')
        REFS.append(REF)

    ACS_API_df['REF'] = REFS

    DF = ACS_API_df.sort_values(by = ['YEAR', 'BASE'], ignore_index = True)

    return DF


def create_acs_api_dataset() -> pd.DataFrame:
    """
    Download the Census Bureau's American Community Survey
    APIs into a formatted dataset.
    """
    URL = 'https://api.census.gov/data/%s' % (os.environ['CENSUS_BUREAU_API_KEY'])
    logger.info("Running request to the Bureau's APIs...")
    CENSUS_DATA_DICT = fetch_content(URL)

    logger.info("Success! Cleaning API information...")

    LIST_ACS_API = [i for i in CENSUS_DATA_DICT['dataset']
                    if '/'.join(i.get('c_dataset')).startswith(('acs/acs'))
                    ]

    DF = _api_df_fmt(LIST_ACS_API)

    logger.info("Successfully formatted ACS API dataframe.")
    
    return DF


def write_api_datasets() -> None:
    """Write the actual API variable reference script."""
    
    df = create_acs_api_dataset()

    FILE_PATH = 'acspsuedo/datasets.py'
    with open(FILE_PATH, 'w') as file:
        file.write(f'''"""
LAST UPDATED: {datetime.now().strftime('%B %d, %Y')}

Metadata for each of the United States Census Bureau
American Community Survey's datasets.

Updated daily.
"""\n\n\n''')
        for REF, BASE in zip(sorted(df['REF'].unique()), sorted(df['BASE'].unique())):
            file.write(f"{REF} = '{BASE}'\n\n")
        file.write('\n\n\n')
        file.write(f"API_METADATA: dict[ str, tuple[ list[int] ] ] = {{\n")
        for BASE in sorted(df['BASE'].unique()):
            mask = df['BASE'] == BASE
            
            REF = df['REF'][mask].iloc[0]
            YEARS = list(df[mask]['YEAR'])
            file.write(f"    {REF}: (\n")
            file.write(f"        {YEARS},\n")
            file.write(f"    ),\n\n")
        file.write(f"}}\n")
        file.write('''"""
Dataset metadata.

Format:
    dataset -> (supported_years)
"""\n\n\n''')
        file.write("SUPPORTED_DATASETS = [\n")
        for REF in sorted(df['REF'].unique()):
            file.write(f"    {REF},\n")
        file.write(']')


    logger.info("Successfully wrote the ACS API script: '~/acspsuedo/datasets.py'")



def main():
    write_api_datasets()


if __name__ == '__main__':
    main()