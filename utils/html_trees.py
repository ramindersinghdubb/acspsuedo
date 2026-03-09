"""
Crawl the TIGER databases.

Executed daily.
"""
from pathlib import Path
from datetime import datetime
import time
import requests as req

from acspsuedo.source import html_spider


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




def census_map_documentation() -> None:
    """
    Fetch documentation on the Census' mapfiles.

    This will contain .txt files containing FIPS codes
    for each of the (when defined) relevant geographic
    scopes. This is useful, moreso, because loading the
    Census database on a browser can be slow.
    """

    FOLDER_PATH = 'crawled_html'
    if not Path(FOLDER_PATH).exists():
        Path(FOLDER_PATH).mkdir(parents = True, exist_ok = True)

    FILE_PATH = f'{FOLDER_PATH}/Census_Map_Documentation.csv'
    if not Path(FILE_PATH).exists():
        logger.info('Census map documentation does not exist. Downloading...')
        ROOT_URL = 'https://www2.census.gov/geo/docs/'
        df = html_spider(url = ROOT_URL)
        df.to_csv(FILE_PATH, sep="|", index=False)
        logger.info('Successfully downloaded map documentation.')
    else:
        logger.info('Census map documentation was already downloaded.')


def tiger_databases() -> None:
    """
    Crawl the TIGER Shapefile databases.
    """
    CURRENT_YEAR = datetime.now().year

    FOLDER_PATH = 'crawled_html/TIGER_databases'
    if not Path(FOLDER_PATH).exists():
        Path(FOLDER_PATH).mkdir(parents = True, exist_ok = True)
        
    for YEAR in range(2008, CURRENT_YEAR + 1):
        TXT_FILE_PATH = f'{FOLDER_PATH}/TIGER{YEAR}_TREE.csv'
        
        if not Path(TXT_FILE_PATH).exists():
            ROOT_URL = f'https://www2.census.gov/geo/tiger/TIGER{YEAR}/'
            https_status = req.get(ROOT_URL).status_code
            
            if https_status == 429:
                logger.error("Too many requests! Restarting...")
                time.sleep(60)
                tiger_databases()
            elif https_status == 200:
                logger.info("Found '%s'. Extracting...", ROOT_URL)
                df = html_spider(ROOT_URL, sleep = 0.5)
                df.to_csv(TXT_FILE_PATH, sep = '|', index = False)
                logger.info("Extracted '%s'. Sleeping...", ROOT_URL)
                time.sleep(60)
            else:
                logger.error("Could not access %s. HTTPS Status Code: %s. Skipping...", ROOT_URL, https_status)
                continue
            
        else:
            logger.info("TIGER %s database was already downloaded.", YEAR)




def main() -> None:
    census_map_documentation()
    tiger_databases()


if __name__ == '__main__':
    main()