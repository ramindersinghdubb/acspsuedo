# [acspsuedo](https://github.com/ramindersinghdubb/acspsuedo/)

[![License](https://img.shields.io/badge/License-MIT-blue)](#license) [![issues - acspsuedo](https://img.shields.io/github/issues/ramindersinghdubb/acspsuedo)](https://github.com/ramindersinghdubb/acspsuedo/issues)

Objects for handling the extraction of American Community Survey data.


## Installation

From source:

```bash
$ git clone https://github.com/ramindersinghdubb/acspsuedo
$ cd acspsuedo
$ pip install .
```

## Usage

See [notebooks](https://github.com/ramindersinghdubb/acspsuedo/tree/main/notebooks/).

<br>

`acspsuedo` handles the extraction of ACS data. For example, if one were interested in the [B25058 "Median Contract Rents" dataset of the American Community Survey's 5-Year Estimates Detailed Tables API at the census tract level for California](https://api.census.gov/data/2024/acs/acs5?get=group(B25058)&for=tract:*&in=state:06), it would be as so.

```python
import acspsuedo.query as apq
from acspsuedo.datasets import ACS5
from acspsuedo.fips.states import CA

df = apq.download(
    dataset = ACS5,
    year    = 2024,
    table   = 'B25058',
    # Geographic specifiers
    state = CA,
    tract = '*'
)
```

*Note that an API key is recommended for querying multiple (50+) datasets in a session. You can obtain a free API key at [https://api.census.gov/data/key_signup.html](https://api.census.gov/data/key_signup.html).*

## Repo Structure

```
acspsuedo/
├── .github/
│   └── workflows/
│       └── acs-api.yml
│
├── acspsuedo/
│   ├── fips/                # Federal Information Processing Standard (FIPS) Codes
│   │   └── ...
│   │
│   ├── source/
│   │   └── ...
│   │
│   ├── __init__.py
│   ├── datasets.py          # Info on supported datasets
│   ├── geog.py
│   └── query.py             # Main access to query ACS data
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
