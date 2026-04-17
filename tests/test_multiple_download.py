"""
Tests for downloads of irregular cases.
"""

import unittest
from pathlib import Path
from functools import partial

import acspsuedo.query as apq
from acspsuedo.datasets import ACS5_SUBJECT
from acspsuedo.fips.states import NORTH_DAKOTA, MINNESOTA, PUERTO_RICO
from acspsuedo.source.low.exceptions import APIException



apq.shapefile_handler.cache_path = Path.cwd() / 'tests' / 'cache'



class TestMultipleDownload(unittest.TestCase):

    def setUp(self) -> None:
        self.DATASET = ACS5_SUBJECT

        self.North_Dakota_CD_download = partial(
            apq.download,
            dataset = ACS5_SUBJECT,
            variables = 'NAME',
            state = NORTH_DAKOTA,
            congressional_district = '*',
            include_geometries = True
        )

        self.Minnesota_PUMA_download = partial(
            apq.download,
            dataset = ACS5_SUBJECT,
            variables = 'NAME',
            state = MINNESOTA,
            public_use_microdata_area = '*',
            include_geometries = True
        )

        self.UA_download = partial(
            apq.download,
            dataset = ACS5_SUBJECT,
            variables = 'NAME',
            urban_area = '*',
            include_geometries = True
        )

    
    def test_lacking_state_outer_scope_nation_wide_congressional_districts_2024_download(self) -> None:
        apq.download(
            dataset = self.DATASET,
            year = 2024,
            variables = 'NAME',
            congressional_district = '*',
            include_geometries = True
        )

    
    def test_ND_congressional_districts_2010_download(self) -> None:
        """
        Tests for congressional districts in North Dakota for 2010.
        """
        self.North_Dakota_CD_download(year = 2010)

    def test_ND_congressional_districts_2014_download(self) -> None:
        """
        Tests for congressional districts in North Dakota for 2014.
        """
        self.North_Dakota_CD_download(year = 2014)

    def test_ND_congressional_districts_2020_download(self) -> None:
        """
        Tests for congressional districts in North Dakota for 2020.
        """
        self.North_Dakota_CD_download(year = 2020)

    def test_ND_congressional_districts_2024_download(self) -> None:
        """
        Tests for congressional districts in North Dakota for 2024.
        """
        self.North_Dakota_CD_download(year = 2024)



    def test_MN_public_use_microdata_area_2008_download(self) -> None:
        """
        Tests for public use microdata areas in Minnesota for 2008.
        """
        with self.assertRaises(APIException):
            self.Minnesota_PUMA_download(year = 2008)
    
    def test_MN_public_use_microdata_area_2010_download(self) -> None:
        """
        Tests for public use microdata areas in Minnesota for 2010.
        """
        self.Minnesota_PUMA_download(year = 2010)

    def test_MN_public_use_microdata_area_2014_download(self) -> None:
        """
        Tests for public use microdata areas in Minnesota for 2014.
        """
        self.Minnesota_PUMA_download(year = 2014)
    
    def test_MN_public_use_microdata_area_2020_download(self) -> None:
        """
        Tests for public use microdata areas in Minnesota for 2020.
        """
        self.Minnesota_PUMA_download(year = 2020)

    def test_MN_public_use_microdata_area_2024_download(self) -> None:
        """
        Tests for public use microdata areas in Minnesota for 2024.

        *Note*: At the time of testing (April 16, 2025), Minnesota PUMA
        geographies aren't returned despite the correct URL (server error?).
        In any case, we use a try-except block.
        """
        try:
            self.Minnesota_PUMA_download(year = 2024)
        except:
            # Returns html content
            with self.assertRaises(APIException):
                self.Minnesota_PUMA_download(year = 2024)



    def test_urban_area_2008_download(self) -> None:
        """
        Tests for urban areas for 2008.
        """
        with self.assertRaises(APIException):
            self.UA_download(year = 2008)

    def test_urban_area_2010_download(self) -> None:
        """
        Tests for urban areas for 2010.
        """
        self.UA_download(year = 2010)

    def test_urban_area_2015_download(self) -> None:
        """
        Tests for urban areas for 2015.
        """
        self.UA_download(year = 2015)

    def test_urban_area_2021_download(self) -> None:
        """
        Tests for urban areas for 2021.
        """
        self.UA_download(year = 2021)

    def test_urban_area_2024_download(self) -> None:
        """
        Tests for urban areas for 2024.
        """
        self.UA_download(year = 2024)



if __name__ == '__main__':
    unittest.main()