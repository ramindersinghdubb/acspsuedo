import acspsuedo.query as apq

from acspsuedo.datasets import ACS5
from acspsuedo.fips.counties.california import Los_Angeles_County
from acspsuedo.fips.states import CA

import pandas as pd


DATASET = ACS5
YEAR = 2020
TABLES = ['B25058', 'B25059']



df = apq.download(
    dataset = DATASET,
    year    = YEAR,
    tables  = TABLES,
    # Geo specifiers
    state   = CA,
    county  = Los_Angeles_County,
    tract   = '*'
)



assert isinstance(df, pd.DataFrame)