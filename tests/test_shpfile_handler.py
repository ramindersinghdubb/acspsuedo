"""
Tests for the shapefile handler interface.
"""

import unittest

from acspsuedo.query import shapefile_handler
from acspsuedo.source.shpfile import ShpfileWarning, ShpfileFormatterException

shapefile_handler.auto_cache = False


class TestShpFileHandler(unittest.TestCase):

    def setUp(self) -> None:

        self.GEOG_SPECS_ZCTA = {'zip_code_tabulation_area': '*'}
        self.ZCTA_2000_URL   = 'https://www2.census.gov/geo/tiger/TIGER2010/ZCTA5/2000/tl_2010_us_zcta500.zip'

        self.GEOG_SPECS_UA = {'urban_area': '*'}
        self.UA_2008_URL   = 'https://www2.census.gov/geo/tiger/TIGER2008/tl_2008_us_uac00.zip'

        self.GEOG_SPECS_GUAM_BG = {'state': '66', 'block_group': '*'}
        self.GUAM_BG_2008_URL   = 'https://www2.census.gov/geo/tiger/TIGER2008/66_GUAM/tl_2008_66_bg00.zip'

        self.GEOG_SPECS_IDAHO_TRACT = {'state': '16', 'tract': '*'}
        self.IDAHO_TRACT_2010_URL   = 'https://www2.census.gov/geo/tiger/TIGER2010/TRACT/2010/tl_2010_16_tract10.zip'

        self.MISSING_GEOG_SPECS_NY_PUMA = {'public_use_microdata_area': '*'}
    

    def test_shpfile_url_constructor_2000_zcta(self) -> None:
        """
        URL constructor for zip-code tabulation areas, 2000.
        """
        url_comps = shapefile_handler._tiger_url_fmtter(year = 2000, **self.GEOG_SPECS_ZCTA)
        url = ''.join(url_comps)

        self.assertEqual(url, self.ZCTA_2000_URL)

    def test_shpfile_url_constructor_2008_guam_block_group(self) -> None:
        """
        URL constructor for block groups in Guam, 2008.
        """
        url_comps = shapefile_handler._tiger_url_fmtter(year = 2008, **self.GEOG_SPECS_GUAM_BG)
        url = ''.join(url_comps)

        self.assertEqual(url, self.GUAM_BG_2008_URL)

    def test_shpfile_url_constructor_2008_ua(self) -> None:
        """
        URL constructor for urban areas, 2008.
        """
        url_comps = shapefile_handler._tiger_url_fmtter(year = 2008, **self.GEOG_SPECS_UA)
        url = ''.join(url_comps)

        self.assertEqual(url, self.UA_2008_URL)

    def test_shpfile_url_constructor_2010_idaho_tracts(self) -> None:
        """
        URL constructor for census tracts in Idaho, 2010.
        """
        url_comps = shapefile_handler._tiger_url_fmtter(year = 2010, **self.GEOG_SPECS_IDAHO_TRACT)
        url = ''.join(url_comps)

        self.assertEqual(url, self.IDAHO_TRACT_2010_URL)



    def test_shpfile_existence_2008_ua(self) -> None:
        """
        A successful TIGER shapefile query for urban areas, 2008.
        """
        shapefile_handler.fetch_tiger_shpfile(year = 2008, **self.GEOG_SPECS_UA)
    
    def test_shpfile_existence_2010_idaho_tracts(self) -> None:
        """
        A successful TIGER shapefile query for census tracts in Idaho, 2010.
        """
        shapefile_handler.fetch_tiger_shpfile(year = 2010, **self.GEOG_SPECS_IDAHO_TRACT)

    def test_failed_shpfile_query_nonexistence_2011_zcta(self) -> None:
        """
        A failed TIGER shapefile query due to non-existence.
        """
        with self.assertWarns(ShpfileWarning):
            shapefile_handler.fetch_tiger_shpfile(year = 2011, **self.GEOG_SPECS_ZCTA)

    def test_failed_shpfile_query_missing_formatters_2024_new_york_puma(self) -> None:
        """
        A failed TIGER shapefile query due to missing formatters.
        """
        with self.assertRaises(ShpfileFormatterException):
            shapefile_handler.fetch_tiger_shpfile(year = 2024, **self.MISSING_GEOG_SPECS_NY_PUMA)


if __name__ == '__main__':
    unittest.main()