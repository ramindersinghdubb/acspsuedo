"""

LAST UPDATED: March 12, 2026

Structured API information for each of the American
Community Survey's APIs.

{Reference: (Base name, available years, supported scopes)}

Contains information on upper-/lower-level geographic scopes
as well.

"""

import typing as t



ACS1 = 'ACS1'

ACS1_CPROFILE = 'ACS1_CPROFILE'

ACS1_PROFILE = 'ACS1_PROFILE'

ACS1_PUMS = 'ACS1_PUMS'

ACS1_PUMSPR = 'ACS1_PUMSPR'

ACS1_SDATAPROFILE_CD119 = 'ACS1_SDATAPROFILE_CD119'

ACS1_SPP = 'ACS1_SPP'

ACS1_SUBJECT = 'ACS1_SUBJECT'

ACS3 = 'ACS3'

ACS3_CPROFILE = 'ACS3_CPROFILE'

ACS3_PROFILE = 'ACS3_PROFILE'

ACS3_SPP = 'ACS3_SPP'

ACS3_SUBJECT = 'ACS3_SUBJECT'

ACS5 = 'ACS5'

ACS5_AIAN = 'ACS5_AIAN'

ACS5_AIANPROFILE = 'ACS5_AIANPROFILE'

ACS5_CPROFILE = 'ACS5_CPROFILE'

ACS5_EEO = 'ACS5_EEO'

ACS5_PROFILE = 'ACS5_PROFILE'

ACS5_PUMS = 'ACS5_PUMS'

ACS5_PUMSPR = 'ACS5_PUMSPR'

ACS5_SPT = 'ACS5_SPT'

ACS5_SPTPROFILE = 'ACS5_SPTPROFILE'

ACS5_SUBJECT = 'ACS5_SUBJECT'

ACSSE = 'ACSSE'




API_DATA: t.Dict[
    str, t.Tuple[str,
                 t.List[int],
                 t.List[str]
                ]
] = {
    'ACS1': (
        'acs/acs1',
        [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
        ['place', 'county', 'state', 'us']
    ),

    'ACS1_CPROFILE': (
        'acs/acs1/cprofile',
        [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
        ['place', 'county', 'state', 'us']
    ),

    'ACS1_PROFILE': (
        'acs/acs1/profile',
        [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
        ['place', 'county', 'state', 'us']
    ),

    'ACS1_PUMS': (
        'acs/acs1/pums',
        [2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
        ['state']
    ),

    'ACS1_PUMSPR': (
        'acs/acs1/pumspr',
        [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
        ['state']
    ),

    'ACS1_SDATAPROFILE_CD119': (
        'acs/acs1/sdataprofile/cd119',
        [2023],
        []
    ),

    'ACS1_SPP': (
        'acs/acs1/spp',
        [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
        ['place', 'county', 'state', 'us']
    ),

    'ACS1_SUBJECT': (
        'acs/acs1/subject',
        [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
        ['place', 'county', 'state', 'us']
    ),

    'ACS3': (
        'acs/acs3',
        [2007, 2008, 2009, 2011, 2012, 2013],
        ['place', 'county', 'state', 'us']
    ),

    'ACS3_CPROFILE': (
        'acs/acs3/cprofile',
        [2012, 2013],
        ['place', 'county', 'state', 'us']
    ),

    'ACS3_PROFILE': (
        'acs/acs3/profile',
        [2007, 2008, 2009, 2010, 2011, 2012, 2013],
        ['place', 'county', 'state', 'us']
    ),

    'ACS3_SPP': (
        'acs/acs3/spp',
        [2009, 2010, 2011, 2012, 2013],
        ['place', 'county', 'state', 'us']
    ),

    'ACS3_SUBJECT': (
        'acs/acs3/subject',
        [2010, 2011, 2012, 2013],
        ['place', 'county', 'state', 'us']
    ),

    'ACS5': (
        'acs/acs5',
        [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
        ['tract', 'place', 'county', 'state', 'us', 'zcta']
    ),

    'ACS5_AIAN': (
        'acs/acs5/aian',
        [2010, 2015, 2021],
        ['state', 'us']
    ),

    'ACS5_AIANPROFILE': (
        'acs/acs5/aianprofile',
        [2010, 2015, 2021],
        ['state', 'us']
    ),

    'ACS5_CPROFILE': (
        'acs/acs5/cprofile',
        [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
        ['place', 'county', 'state', 'us']
    ),

    'ACS5_EEO': (
        'acs/acs5/eeo',
        [2018],
        ['place', 'county', 'state', 'us']
    ),

    'ACS5_PROFILE': (
        'acs/acs5/profile',
        [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
        ['tract', 'place', 'county', 'state', 'us', 'zcta']
    ),

    'ACS5_PUMS': (
        'acs/acs5/pums',
        [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
        ['state']
    ),

    'ACS5_PUMSPR': (
        'acs/acs5/pumspr',
        [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
        ['state']
    ),

    'ACS5_SPT': (
        'acs/acs5/spt',
        [2010, 2015, 2021],
        ['tract', 'place', 'county', 'state', 'us']
    ),

    'ACS5_SPTPROFILE': (
        'acs/acs5/sptprofile',
        [2010, 2021],
        ['tract', 'place', 'county', 'state', 'us']
    ),

    'ACS5_SUBJECT': (
        'acs/acs5/subject',
        [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
        ['tract', 'place', 'county', 'state', 'us', 'zcta']
    ),

    'ACSSE': (
        'acs/acsse',
        [2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
        ['place', 'county', 'state', 'us']
    ),

}



"""
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
