# [acspsuedo](https://github.com/ramindersinghdubb/acspsuedo/)

[![License](https://img.shields.io/badge/License-MIT-blue)](#license) [![issues - acspsuedo](https://img.shields.io/github/issues/ramindersinghdubb/acspsuedo)](https://github.com/ramindersinghdubb/acspsuedo/issues)

Objects for handling the extraction of American Community Survey data.


## Installation

```bash
$ git clone https://github.com/ramindersinghdubb/acspsuedo
$ cd acspsuedo
$ pip install .
```

## Usage

See [notebooks](https://ramindersinghdubb.github.io/acspsuedo/tree/mainnotebooks/).

<br>

`acspsuedo` handles the extraction of ACS data. For example, if one were interested in the [B25058 "Median Contract Rents" dataset of the American Community Survey's 5-Year Estimates Detailed Tables API at the census tract level for California](https://api.census.gov/data/2024/acs/acs5?get=group(B25058)&for=tract:*&in=state:06), it would be as so.

```python
from acspsuedo.query import download

from acspsuedo.api import ACS5
from acspsuedo.fips.states import CA
from acspsuedo.fips.places.CA import LOS_ANGELES

df = download(
    dataset   = ACS5,
    year      = 2024,
    table = 'B25058',
    state = CA,
    tract = '*'
)
```

*Note that an API key is recommended for querying multiple (50+) datasets in a session. You can obtain a free API key at [https://api.census.gov/data/key_signup.html](https://api.census.gov/data/key_signup.html).*

<br>

~~You can also specify whether or not to include optional geographic information from the [Census Bureau's TIGER Shapefile database](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html) with the `include_geometries` argument.~~

~~Note that TIGER shapefiles and/or the supplied API key are automatically cached in the current working directory (`./cache/`). Caching preferences may be disabled by updating the properties of the `CONFIG` object.~~

**SHAPEFILE/GEOGRAPHIC HANDLING IS IN DEVELOPMENT.**

## Repo Structure

```
acspsuedo/
├── .github/
│   └── workflows/
│       └── acs-api.yml
│
├── acspsuedo/
│   ├── fips/                  # Federal Information Processing Standard (FIPS) Codes
│   │   └── ...
│   │
│   ├── source/
│   │   └── ...
│   │
│   ├── __init__.py
│   ├── datasets.py            # Info on supported datasets
│   ├── geog.py
│   └── query.py               # Main access to query ACS data
│
├── notebooks/
│   └── ...
│
├── tests/
│   └── ...
│
├── utils/
│   └── ...
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
