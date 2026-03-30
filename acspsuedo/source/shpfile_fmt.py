"""
Data/shapefile formatting.
"""

import typing as t



GEO_SPEC_METADATA: t.Dict[
    str, t.Tuple[t.Optional[str],
                 t.List,
                 t.Optional[str],
                 t.Optional[t.List[str]],
                 t.Optional[str]
                 ]
] = {
    'us': (
        '010',
        ['US'],
        None,
        None,
        None
    ),
    'region': (
        '020',
        ['REGION'],
        None,
        None,
        None
    ),
    'division': (
        '030',
        ['DIVISION'],
        'STATE',
        None,
        None
    ),
    'state': (
        '040',
        ['STATE'],
        'STATE',
        ['STATEFP'],
        'us'
    ),
    'county': (
        '050',
        ['STATE', 'COUNTY'],
        'COUNTY',
        ['STATEFP', 'COUNTYFP'],
        'us',
    ),
    'county_subdivision': (
        '060',
        ['STATE', 'COUNTY', 'COUNTY_SUBDIVISION'],
        'COUSUB',
        ['STATEFP', 'COUNTYFP', 'COUSUBFP'],
        'state'
    ),
    'subminor_civil_division': (
        '067',
        ['STATE', 'COUNTY', 'COUNTY_SUBDIVISION', 'SUBMINOR_CIVIL_DIVISION'],
        'SUBMCD',
        ['STATEFP', 'COUNTYFP', 'COUSUBFP', 'SUBMCDFP'],
        'state'
    ),
    'place_remainder_or_part': (
        '070',
        ['STATE', 'COUNTY', 'PLACE'],
        None,
        None,
        None
    ),
    'tract': (
        '140',
        ['STATE', 'COUNTY', 'TRACT'],
        'TRACT',
        ['STATEFP', 'COUNTYFP', 'TRACTCE'],
        'state'
    ),
    'block_group': (
        '150',
        ['STATE', 'COUNTY', 'TRACT', 'BLOCK_GROUP'],
        'BG',
        ['STATEFP', 'COUNTYFP', 'TRACTCE', 'BLKGRPCE'],
        'state'
    ),
    'county_or_part': (
        '155',
        ['STATE', 'COUNTY'],
        None,
        None,
        None
    ),
    'place': (
        '160',
        ['STATE', 'PLACE'],
        'PLACE',
        ['STATEFP', 'PLACEFP'],
        'state'
    ),
    'consolidated_city': (
        '170',
        ['STATE', 'CONSOLIDATED_CITY'],
        'CONCITY',
        ['STATEFP', 'CONCTYFP'],
        'state'
    ),
    'place_or_part': (
        '172',
        ['STATE', 'PLACE'],
        None,
        None,
        None
    ),
    'alaska_native_regional_corporation': (
        '230',
        ['STATE', 'ALASKA_NATIVE_REGIONAL_CORPORATION'],
        'ANRC',
        ['STATEFP', 'ANRCFP'],
        'state'
    ),
    'american_indian_area_alaska_native_area_hawaiian_home_land': (
        '250',
        ['AMERICAN_INDIAN_AREA_ALASKA_NATIVE_AREA_HAWAIIAN_HOME_LAND'],
        'AIANNH',
        ['AIANNHCE'],
        'us'
    ),
    'tribal_subdivision_remainder': (
        '251',
        ['AMERICAN_INDIAN_AREA_ALASKA_NATIVE_AREA_HAWAIIAN_HOME_LAND', 'TRIBAL_SUBDIVISION_REMAINDER'],
        None,
        None,
        None
    ),
    'american_indian_area_alaska_native_area_reservation_or_statistical_entity_only': (
        '252',
        ['AMERICAN_INDIAN_AREA_ALASKA_NATIVE_AREA_RESERVATION_OR_STATISTICAL_ENTITY_ONLY'],
        None,
        None,
        None
    ),
    'american_indian_area_off_reservation_trust_land_only_hawaiian_home_land': (
        '254',
        ['AMERICAN_INDIAN_AREA_OFF_RESERVATION_TRUST_LAND_ONLY_HAWAIIAN_HOME_LAND'],
        None,
        None,
        None
    ),
    'tribal_census_tract': (
        '256',
        ['TRIBAL_CENSUS_TRACT'],
        'TTRACT',
        ['AIANNHCE', 'TTRACTCE'],
        'us'
    ),
    'tribal_block_group': (
        '258',
        ['TRIBAL_BLOCK_GROUP'],
        'TBG',
        ['AIANNHCE', 'TTRACTCE', 'TBLKGPCE'],
        'us'
    ),
    'state_or_part': (
        '260',
        ['STATE_OR_PART'],
        None,
        None,
        None
    ),
    'american_indian_area_alaska_native_area_hawaiian_home_land_or_part': (
        '280',
        ['STATE', 'AMERICAN_INDIAN_AREA_ALASKA_NATIVE_AREA_HAWAIIAN_HOME_LAND_OR_PART'],
        None,
        None,
        None
    ),
    'american_indian_area_alaska_native_area_reservation_or_statistical_entity_only_or_part': (
        '283',
        ['STATE', 'american_indian_area_alaska_native_area_reservation_or_statistical_entity_only_or_part'],
        None,
        None,
        None
    ),
    'tribal_census_tract_or_part': (
        '283',
        ['TRIBAL_CENSUS_TRACT_OR_PART'],
        None,
        None,
        None
    ),
    'tribal_block_group_or_part': (
        '286',
        ['TRIBAL_BLOCK_GROUP_OR_PART'],
        None,
        None,
        None
    ),
    'metropolitan_statistical_area_micropolitan_statistical_area': (
        '310',
        ['METROPOLITAN_STATISTICAL_AREA_MICROPOLITAN_STATISTICAL_AREA'],
        'CBSA',
        ['CBSAFP'],
        'us'
    ),
    'principal_city_or_part': (
        '312',
        ['PRINCIPAL_CITY_OR_PART'],
        None,
        None,
        None
    ),
    'metropolitan_division': (
        '314',
        ['METROPOLITAN_DIVISION'],
        'METDIV',
        ['CBSAFP', 'METDIVFP'],
        'us'
    ),
    'metropolitan_statistical_area_micropolitan_statistical_area_or_part': (
        '320',
        ['METROPOLITAN_STATISTICAL_AREA_MICROPOLITAN_STATISTICAL_AREA_OR_PART'],
        None,
        None,
        None
    ),
    'metropolitan_division_or_part': (
        '323',
        ['METROPOLITAN_DIVISION_OR_PART'],
        None,
        None,
        'us'
    ),
    'combined_statistical_area': (
        '330',
        ['COMBINED_STATISTICAL_AREA'],
        'CSA',
        ['CSAFP'],
        'us'
    ),
    'combined_new_england_city_and_town_area': (
        '335',
        ['COMBINED_NEW_ENGLAND_CITY_AND_TOWN_AREA'],
        'CNECTA',
        ['CNECTAFP'],
        'us'
    ),
    'new_england_city_and_town_area': (
        '350',
        ['NEW_ENGLAND_CITY_AND_TOWN_AREA'],
        'NECTA',
        ['CNECTAFP', 'NECTAFP'],
        'us'
    ),
    'combined_statistical_area_or_part': (
        '340',
        ['STATE', 'COMBINED_STATISTICAL_AREA_OR_PART'],
        None,
        None,
        None
    ),
    'combined_new_england_city_and_town_area_or_part': (
        '',
        ['COMBINED_NEW_ENGLAND_CITY_AND_TOWN_AREA_OR_PART'],
        None,
        None,
        None
    ),
    'new_england_city_and_town_area_or_part': (
        '',
        ['NEW_ENGLAND_CITY_AND_TOWN_AREA_OR_PART'],
        None,
        None,
        None
    ),
    'principal_city': (
        '352',
        ['PRINCIPAL_CITY'],
        None,
        None,
        None
    ),
    'necta_division': (
        '355',
        ['NECTA_DIVISION'],
        'NECTADIV',
        ['CNECTAFP', 'NECTAFP', 'NCTADVFP'],
        'us'
    ),
    'necta_division_or_part': (
        '',
        ['NECTA_DIVISION_OR_PART'],
        None,
        None,
        None
    ),
    'urban_area': (
        '400',
        ['URBAN_AREA'],
        'UAC',
        ['UACE'],
        'us'
    ),
    'congressional_district': (
        '500',
        ['STATE', 'CONGRESSIONAL_DISTRICT'],
        'CD',
        ['STATEFP', 'CDFP', 'GEOID'],
        'us'
    ),
    'state_legislative_district_upper_chamber': (
        '610',
        ['STATE', 'STATE_LEGISLATIVE_DISTRICT_UPPER_CHAMBER'],
        'SLDU',
        ['STATEFP', 'SLDUST'],
        'state'
    ),
    'state_legislative_district_lower_chamber': (
        '620',
        ['STATE', 'STATE_LEGISLATIVE_DISTRICT_LOWER_CHAMBER'],
        'SLDL',
        ['STATEFP', 'SLDLST'],
        'state'
    ),
    'public_use_microdata_area': (
        '795',
        ['STATE', 'PUBLIC_USE_MICRODATA_AREA', 'SERIALNO', 'SPORDER'],
        'PUMA',
        ['STATEFP', 'PUMACE'],
        'state'
    ),
    'zip_code_tabulation_area': (
        '860',
        ['ZIP_CODE_TABULATION_AREA'],
        'ZCTA',
        ['ZCTACE'],
        'us'
    ),
    'school_district_elementary': (
        '950',
        ['STATE', 'SCHOOL_DISTRICT_ELEMENTARY'],
        'ELSD',
        ['STATEFP', 'ELSDLEA', 'LOGRADE', 'HIGRADE'],
        'state'
    ),
    'school_district_secondary': (
        '960',
        ['STATE', 'SCHOOL_DISTRICT_SECONDARY'],
        'SCSD',
        ['STATEFP', 'SCSDLEA', 'LOGRADE', 'HIGRADE'],
        'state'
    ),
    'school_district_unified': (
        '970',
        ['STATE', 'SCHOOL_DISTRICT_UNIFIED'],
        'UNSD',
        ['STATEFP', 'UNSDLEA', 'LOGRADE', 'HIGRADE'],
        'state'
    )
}
"""
Custom dictionary specifying metadata on geographic specifiers.

    Key -> (reference_code, df_columns, shpfile_scope_indicator, shpfile_columns, outer_scope_reference)
"""