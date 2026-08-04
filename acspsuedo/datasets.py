"""
LAST UPDATED: August 04, 2026

Metadata for each of the United States Census Bureau
American Community Survey's datasets.

Updated daily.
"""


ACS1 = 'acs/acs1'

ACS1_CPROFILE = 'acs/acs1/cprofile'

ACS1_PROFILE = 'acs/acs1/profile'

ACS1_PUMS = 'acs/acs1/pums'

ACS1_PUMSPR = 'acs/acs1/pumspr'

ACS1_SDATAPROFILE_CD119 = 'acs/acs1/sdataprofile/cd119'

ACS1_SPP = 'acs/acs1/spp'

ACS1_SUBJECT = 'acs/acs1/subject'

ACS3 = 'acs/acs3'

ACS3_CPROFILE = 'acs/acs3/cprofile'

ACS3_PROFILE = 'acs/acs3/profile'

ACS3_SPP = 'acs/acs3/spp'

ACS3_SUBJECT = 'acs/acs3/subject'

ACS5 = 'acs/acs5'

ACS5_AIAN = 'acs/acs5/aian'

ACS5_AIANPROFILE = 'acs/acs5/aianprofile'

ACS5_CPROFILE = 'acs/acs5/cprofile'

ACS5_EEO = 'acs/acs5/eeo'

ACS5_PROFILE = 'acs/acs5/profile'

ACS5_PUMS = 'acs/acs5/pums'

ACS5_PUMSPR = 'acs/acs5/pumspr'

ACS5_SPT = 'acs/acs5/spt'

ACS5_SPTPROFILE = 'acs/acs5/sptprofile'

ACS5_SUBJECT = 'acs/acs5/subject'

ACSSE = 'acs/acsse'




API_METADATA: dict[ str, tuple[ list[int] ] ] = {
    ACS1: (
        [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
    ),

    ACS1_CPROFILE: (
        [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
    ),

    ACS1_PROFILE: (
        [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
    ),

    ACS1_PUMS: (
        [2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
    ),

    ACS1_PUMSPR: (
        [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
    ),

    ACS1_SDATAPROFILE_CD119: (
        [2023],
    ),

    ACS1_SPP: (
        [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
    ),

    ACS1_SUBJECT: (
        [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
    ),

    ACS3: (
        [2007, 2008, 2009, 2011, 2012, 2013],
    ),

    ACS3_CPROFILE: (
        [2012, 2013],
    ),

    ACS3_PROFILE: (
        [2007, 2008, 2009, 2010, 2011, 2012, 2013],
    ),

    ACS3_SPP: (
        [2009, 2010, 2011, 2012, 2013],
    ),

    ACS3_SUBJECT: (
        [2010, 2011, 2012, 2013],
    ),

    ACS5: (
        [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
    ),

    ACS5_AIAN: (
        [2010, 2015, 2021],
    ),

    ACS5_AIANPROFILE: (
        [2010, 2015, 2021],
    ),

    ACS5_CPROFILE: (
        [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
    ),

    ACS5_EEO: (
        [2018],
    ),

    ACS5_PROFILE: (
        [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
    ),

    ACS5_PUMS: (
        [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
    ),

    ACS5_PUMSPR: (
        [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
    ),

    ACS5_SPT: (
        [2010, 2015, 2021],
    ),

    ACS5_SPTPROFILE: (
        [2010, 2021],
    ),

    ACS5_SUBJECT: (
        [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
    ),

    ACSSE: (
        [2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024],
    ),

}
"""
Dataset metadata.

Format:
    dataset -> (supported_years)
"""


SUPPORTED_DATASETS = [
    ACS1,
    ACS1_CPROFILE,
    ACS1_PROFILE,
    ACS1_PUMS,
    ACS1_PUMSPR,
    ACS1_SDATAPROFILE_CD119,
    ACS1_SPP,
    ACS1_SUBJECT,
    ACS3,
    ACS3_CPROFILE,
    ACS3_PROFILE,
    ACS3_SPP,
    ACS3_SUBJECT,
    ACS5,
    ACS5_AIAN,
    ACS5_AIANPROFILE,
    ACS5_CPROFILE,
    ACS5_EEO,
    ACS5_PROFILE,
    ACS5_PUMS,
    ACS5_PUMSPR,
    ACS5_SPT,
    ACS5_SPTPROFILE,
    ACS5_SUBJECT,
    ACSSE,
]