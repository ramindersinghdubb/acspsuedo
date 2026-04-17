"""
Tests for the download functions.
"""

import unittest
from pathlib import Path

import pandas as pd
import geopandas as gpd

import acspsuedo.query as apq
from acspsuedo.datasets import ACS5_PROFILE
from acspsuedo.fips.states import NY
from acspsuedo.fips.places.new_york import Albany



apq.shapefile_handler.cache_path = Path.cwd() / 'tests' / 'cache'



class TestDownload(unittest.TestCase):

    def setUp(self) -> None:
        self.DATASET = ACS5_PROFILE
        self.YEAR    = 2009
        self.VARS    = ['DP04_0016E']

    def test_confined_query_arg_space(self):
        """
        Test the argument space for the confined query interface.
        """
        with self.assertRaises(ValueError):
            apq.confined_download(-0.1)
        with self.assertRaises(ValueError):
            apq.confined_download(1.1)

    def test_confined_query_instance_equality(self):
        """
        Simulate a scenario where confined query interfaces are equal/unequal.
        """
        query1 = apq.confined_download(0.8, state = NY, place = Albany)
        query2 = apq.confined_download(0.80, place = Albany, state = NY)

        self.assertEqual(query1, query2)

        query3 = apq.confined_download(0.1, state = NY)

        self.assertNotEqual(query3, query1)

    def test_confined_query_instance(self):
        """
        Simulate a scenario where a user introspects the confined query interface.
        """
        query1 = apq.confined_download(0.8, state = NY, place = Albany)

        self.assertEqual(query1.area_threshold, 0.8)

        with self.assertRaises(ValueError):
            query1.area_threshold = -0.1

        query1.area_threshold = 0.5

        self.assertTrue(query1.geographic_specifiers == {'place': Albany, 'state': NY})

        self.assertFalse(query1 == dict(state = NY, place = Albany))

        query1.geographic_specifiers = {'state': NY}

        self.assertTrue(str(query1) == '_ConfinedDownload(area_threshold = 0.5, geographic_specifiers = {state = 36})')

    def test_confined_query_download(self):
        cfi = apq.confined_download(0.8, state = NY, place = Albany)

        gdf = cfi.download(
            self.DATASET,
            self.YEAR,
            variables = self.VARS,
            include_geometries = True,
            state = NY,
            tract = '*'
        )

        self.assertIsInstance(gdf, gpd.GeoDataFrame)

        df = cfi.download(
            self.DATASET,
            self.YEAR,
            variables = self.VARS,
            state = NY,
            tract = '*'
        )

        self.assertIsInstance(df, pd.DataFrame)

    # NOTE:
    # Due to our deterministic rules, we have provided

    def test_confined_query_download_without_inner_geometries(self):
        """
        Simulate a known scenario whereby the geographic information for the
        inner-layer set of geographies cannot be found or is non-existent.

        The example queries data for ZCTAs within New York's 11th congressional
        district for the congressional session corresponding to 2011.
        """
        cfq = apq.confined_download(0.8, state = NY, congressional_district = '11')

        with self.assertWarns(UserWarning):
            cfq.download(
                self.DATASET,
                2011,
                variables = self.VARS,
                zip_code_tabulation_area = '*',
            )

    def test_confined_query_download_without_outer_geometries(self):
        """
        Simulate a known scenario whereby the geographic information for the
        outer-layer set of geographies cannot be found or is non-existent.

        The example queries data for all states in the New England division.
        Cf. https://www2.census.gov/geo/pdfs/maps-data/maps/reference/us_regdiv.pdf
        """
        cfq = apq.confined_download(0.8, division = '2')

        with self.assertWarns(UserWarning):
            cfq.download(
                self.DATASET,
                2016,
                variables = self.VARS,
                state = '*',
            )



if __name__ == '__main__':
    unittest.main()