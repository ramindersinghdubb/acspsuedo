# [acspsuedo](https://ramindersinghdubb.github.io/acspsuedo/)

[![License](https://img.shields.io/badge/License-MIT-blue)](#license) [![issues - acspsuedo](https://img.shields.io/github/issues/ramindersinghdubb/acspsuedo)](https://github.com/ramindersinghdubb/acspsuedo/issues)

Objects for handling the extraction of American Community Survey data on collections of geographies.


## Installation

```bash
$ git clone https://github.com/ramindersinghdubb/acspsuedo
$ cd acspsuedo
$ pip install .
```

## Usage

See [notebooks](https://ramindersinghdubb.github.io/acspsuedo/tree/mainnotebooks/).

<br>

`acspsuedo` handles the extraction of ACS data for a collection of geographies. Thus, if one were interested in the [B25058 "Median Contract Rents" dataset of the American Community Survey's 5-Year Estimates Detailed Tables API at the census tract level for Los Angeles, California](https://data.census.gov/table?q=B25058&g=160XX00US0644000$1400000), it would be as so.

```python
from acspsuedo.data import download_by_geo_collection

from acspsuedo.api import ACS5
from acspsuedo.fips.states import CA
from acspsuedo.fips.places.CA import LOS_ANGELES

df = download_by_geo_collection(
    api       = ACS5,
    year      = 2024,
    dataset   = 'B25058',
    geography = 'tract',
    state = CA,
    place = LOS_ANGELES
)
```

*Note that an API key is recommended for querying multiple (50+) datasets in a session. You can obtain a free API key at [https://api.census.gov/data/key_signup.html](https://api.census.gov/data/key_signup.html).*

<br>

You can also specify whether or not to include optional geographic information from the [Census Bureau's TIGER Shapefile database](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html) with the `include_geometries` argument.

```python
gdf = download_by_geo_collection(
    api                = ACS5,
    year               = 2024,
    dataset            = 'B25058',
    geography          = 'tract',
    include_geometries = True,
    state = CA,
    place = LOS_ANGELES
)
```

Note that TIGER shapefiles and/or the supplied API key are automatically cached in the current working directory (`./cache/`). Caching preferences may be disabled by updating the properties of the `CONFIG` object.

```python
CONFIG.is_cache = False

CONFIG.api_key = 'your_api_key'

CONFIG.is_cache = True
CONFIG.cache_path = 'new_cache_path'
```

## Repo Structure

```python
acspsuedo/
├── .github/
│   └── workflows/
│       └── python-scripts.yml
│
├── acspsuedo/
│   ├── fips/                            # Federal Information Processing Standard (FIPS) Codes
│   │   ├── counties/
│   │   │   └── ...
│   │   ├── places/
│   │   │   └── ...
│   │   ├── __init__.py
│   │   ├── states.py
│   │   └── ...
│   │
│   ├── source/                          # Lower-level
│   │   └── ...
│   │
│   ├── __init__.py
│   ├── api.py                           # Info on supported APIs
│   ├── config.py
│   ├── data.py                          # High-level objects to download ACS data
│   └── geographies.py                   # TIGER Shapefiles for select geographic scopes
│
├── crawled_html/
│   ├── TIGER_databases/
│   │   └── ...
│   └── Census_Map_Documentation.csv
│
├── notebooks/
│   └── ...
│
├── tests/
│   └── ...
│
├── utils/
│   ├── __init__.py
│   ├── api_metadata.py
│   ├── fips.py
│   └── html_trees.py
│
├── .gitattributes
├── .gitignore
├── LICENSE
├── poetry.lock
├── pyproject.toml
└── README.md
```

## Licensing

`acspsuedo` is licensed under the terms and conditions of the `MIT` license.