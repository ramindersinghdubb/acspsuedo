"""
Data/shapefile formatting.
"""

import typing as t



GEO_SPEC_METADATA: t.Dict[
    str, t.Tuple[t.Optional[str],
                 t.List
                 ]
] = {
    'us': (
        '010',
        ['US']
    ),
    'region': (
        '020',
        ['REGION']
    ),
    'division': (
        '030',
        ['DIVISION']
    ),
    'state': (
        '040',
        ['STATE']
    ),
    'county': (
        '050',
        ['STATE', 'COUNTY']
    ),
    'county_subdivision': (
        '060',
        ['STATE', 'COUNTY', 'COUNTY_SUBDIVISION']
    ),
    'subminor_civil_division': (
        '067',
        ['STATE', 'COUNTY', 'COUNTY_SUBDIVISION', 'SUBMINOR_CIVIL_DIVISION']
    ),
    'place_remainder_or_part': (
        '070',
        ['STATE', 'COUNTY', 'PLACE']
    ),
    'tract': (
        '140',
        ['STATE', 'COUNTY', 'TRACT']
    ),
    'block_group': (
        '150',
        ['STATE', 'COUNTY', 'TRACT', 'BLOCK_GROUP']
    ),
    'county_or_part': (
        '155',
        ['STATE', 'COUNTY']
    ),
    'place': (
        '160',
        ['STATE', 'PLACE']
    ),
    'consolidated_city': (
        '170',
        ['STATE', 'CONSOLIDATED_CITY']
    ),
    'place_or_part': (
        '172',
        ['STATE', 'PLACE']
    ),
    'alaska_native_regional_corporation': (
        '230',
        ['STATE', 'ALASKA_NATIVE_REGIONAL_CORPORATION']
    ),
    'american_indian_area_alaska_native_area_hawaiian_home_land': (
        '250',
        ['AMERICAN_INDIAN_AREA_ALASKA_NATIVE_AREA_HAWAIIAN_HOME_LAND']
    ),
    'tribal_subdivision_remainder': (
        '251',
        ['AMERICAN_INDIAN_AREA_ALASKA_NATIVE_AREA_HAWAIIAN_HOME_LAND', 'TRIBAL_SUBDIVISION_REMAINDER']
    ),
    'american_indian_area_alaska_native_area_reservation_or_statistical_entity_only': (
        '252',
        ['AMERICAN_INDIAN_AREA_ALASKA_NATIVE_AREA_RESERVATION_OR_STATISTICAL_ENTITY_ONLY']
    ),
    'american_indian_area_off_reservation_trust_land_only_hawaiian_home_land': (
        '254',
        ['AMERICAN_INDIAN_AREA_OFF_RESERVATION_TRUST_LAND_ONLY_HAWAIIAN_HOME_LAND']
    ),
    'tribal_census_tract': (
        '256',
        ['TRIBAL_CENSUS_TRACT']
    ),
    'tribal_block_group': (
        '258',
        ['TRIBAL_BLOCK_GROUP']
    ),
    'state_or_part': (
        '260',
        ['STATE_OR_PART']
    ),
    'american_indian_area_alaska_native_area_hawaiian_home_land_or_part': (
        '280',
        ['STATE', 'AMERICAN_INDIAN_AREA_ALASKA_NATIVE_AREA_HAWAIIAN_HOME_LAND_OR_PART']
    ),
    'american_indian_area_alaska_native_area_reservation_or_statistical_entity_only_or_part': (
        '283',
        ['STATE', 'american_indian_area_alaska_native_area_reservation_or_statistical_entity_only_or_part']
    ),
    'AMERICAN_INDIAN_AREA_ALASKA_NATIVE_AREA_RESERVATION_OR_STATISTICAL_ENTITY_ONLY_OR_PART': (
        '286',
        ['STATE', 'AMERICAN_INDIAN_AREA_OFF_RESERVATION_TRUST_LAND_ONLY_HAWAIIAN_HOME_LAND_OR_PART']
    ),
    'tribal_census_tract_or_part': (
        '283',
        ['TRIBAL_CENSUS_TRACT_OR_PART']
    ),
    'tribal_block_group_or_part': (
        '286',
        ['TRIBAL_BLOCK_GROUP_OR_PART']
    ),
    'metropolitan_statistical_area_micropolitan_statistical_area': (
        '310',
        ['METROPOLITAN_STATISTICAL_AREA_MICROPOLITAN_STATISTICAL_AREA']
    ),
    'principal_city_or_part': (
        '312',
        ['PRINCIPAL_CITY_OR_PART']
    ),
    'metropolitan_division': (
        '314',
        ['METROPOLITAN_DIVISION']
    ),
    'metropolitan_statistical_area_micropolitan_statistical_area_or_part': (
        '320',
        ['METROPOLITAN_STATISTICAL_AREA_MICROPOLITAN_STATISTICAL_AREA_OR_PART']
    ),
    'metropolitan_division_or_part': (
        '323',
        ['METROPOLITAN_DIVISION_OR_PART']
    ),
    'combined_statistical_area': (
        '330',
        ['COMBINED_STATISTICAL_AREA']
    ),
    'combined_new_england_city_and_town_area': (
        '335',
        ['COMBINED_NEW_ENGLAND_CITY_AND_TOWN_AREA']
    ),
    'new_england_city_and_town_area': (
        '350',
        ['NEW_ENGLAND_CITY_AND_TOWN_AREA']
    ),
    'combined_statistical_area_or_part': (
        '340',
        ['STATE', 'COMBINED_STATISTICAL_AREA_OR_PART']
    ),
    'combined_new_england_city_and_town_area_or_part': (
        '',
        ['COMBINED_NEW_ENGLAND_CITY_AND_TOWN_AREA_OR_PART']
    ),
    'new_england_city_and_town_area_or_part': (
        '',
        ['NEW_ENGLAND_CITY_AND_TOWN_AREA_OR_PART']
    ),
    'principal_city': (
        '352',
        ['PRINCIPAL_CITY']
    ),
    'necta_division': (
        '355',
        ['NECTA_DIVISION']
    ),
    'necta_division_or_part': (
        '',
        ['NECTA_DIVISION_OR_PART']
    ),
    'urban_area': (
        '400',
        ['URBAN_AREA']
    ),
    'congressional_district': (
        '500',
        ['STATE', 'CONGRESSIONAL_DISTRICT']
    ),
    'state_legislative_district_upper_chamber': (
        '610',
        ['STATE', 'STATE_LEGISLATIVE_DISTRICT_UPPER_CHAMBER']
    ),
    'state_legislative_district_lower_chamber': (
        '620',
        ['STATE', 'STATE_LEGISLATIVE_DISTRICT_LOWER_CHAMBER']
    ),
    'public_use_microdata_area': (
        '795',
        ['STATE', 'PUBLIC_USE_MICRODATA_AREA']
    ),
    'zip_code_tabulation_area': (
        '860',
        ['ZIP_CODE_TABULATION_AREA']
    ),
    'school_district_elementary': (
        '950',
        ['STATE', 'SCHOOL_DISTRICT_ELEMENTARY']
    ),
    'school_district_secondary': (
        '960',
        ['STATE', 'SCHOOL_DISTRICT_SECONDARY']
    ),
    'school_district_unified': (
        '970',
        ['STATE', 'SCHOOL_DISTRICT_UNIFIED']
    )
}
"""
Custom dictionary specifying metadata on geographic specifiers.

    Key -> (reference_code, df_columns)
"""