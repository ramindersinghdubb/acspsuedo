"""
Tests for the download functions.
"""

import unittest

import aiohttp
import pandas as pd

import acspsuedo.query as apq
from acspsuedo.source.low.exceptions import APIException
from acspsuedo.source.geog import GeoScopeException
from acspsuedo.datasets import ACS1, ACS5
from acspsuedo.fips.states import CA



class TestDownload(unittest.TestCase):

    def setUp(self) -> None:
        self.DATASET   = ACS5
        self.YEAR      = 2020
        self.VARIABLES = ['NAME', 'GEO_ID', 'B25058_001E']

        self.CONTENT = [
            ['NAME', 'GEO_ID', 'STATE', 'YEAR', 'B25058_001E'],
            ['California', '06', '06', 2020, 1442]
        ]
        self.DF = pd.DataFrame(
            columns = self.CONTENT[0],
            data = self.CONTENT[1:]
        )

        self.ACS1_DATASET = ACS1


    def test_download_url(self):
        """
        Formatted URL for sending queries.
        """
        key_fmt = apq.api_key_config._get_api_key()
        URL = f'https://api.census.gov/data/2020/acs/acs5?get=B25058_001E,GEO_ID,NAME&for=state:06{key_fmt}'
        fmt_url, _, _ = apq._fmt_download_url(
            self.DATASET,
            self.YEAR,
            vars = self.VARIABLES,
            state = CA
        )
        self.assertEqual(URL, fmt_url[0])


    def test_download_return_type(self):
        """
        Synchronous download.
        """
        df = apq.download(
            self.DATASET,
            self.YEAR,
            variables = self.VARIABLES,
            state = CA
        )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual([self.CONTENT[1]], df.values.tolist())

    def test_download_geometries_non_existent_shpfile(self):
        """
        A scenario where users query data alongside their geographic info,
        but TIGER shapefiles are not available.
        """
        with self.assertWarns(UserWarning):
            apq.download(
                self.DATASET,
                2011,
                variables = self.VARIABLES,
                zip_code_tabulation_area = '*',
                include_geometries = True
            )

    def test_failed_download_unsupported_dataset(self):
        """
        A failed download due to an unsupported dataset.
        """
        with self.assertRaises(KeyError):
            df = apq.download(
                'foo/bar',
                self.YEAR,
                variables = self.VARIABLES,
                state = 'bar'
            )

    def test_failed_download_unsupported_year(self):
        """
        A failed download due to an unsupported year.
        """
        with self.assertRaises(APIException):
            df = apq.download(
                self.ACS1_DATASET,
                self.YEAR,
                variables = self.VARIABLES,
                state = CA
            )

    def test_failed_download_unsupported_geo_specifier(self):
        """
        A failed download due to an unsupported geographic specifier.
        """
        with self.assertRaises(GeoScopeException):
            df = apq.download(
                self.DATASET,
                self.YEAR,
                variables = self.VARIABLES,
                foo = 'bar'
            )

    def test_failed_download_incorrect_geo_specifier_value(self):
        """
        A failed download due to an incorrect geographic specifier value.
        """
        with self.assertRaises(APIException):
            df = apq.download(
                self.DATASET,
                self.YEAR,
                variables = self.VARIABLES,
                state = 'foo'
            )


class TestAsyncDownload(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.DATASET   = ACS5
        self.YEAR      = 2020
        self.VARIABLES = ['NAME', 'GEO_ID', 'B25058_001E']

        self.CONTENT = [
            ['NAME', 'GEO_ID', 'STATE', 'YEAR', 'B25058_001E'],
            ['California', '06', '06', 2020, 1442]
        ]
        self.DF = pd.DataFrame(
            columns = self.CONTENT[0],
            data = self.CONTENT[1:]
        )

    async def asyncSetUp(self):
        self.ASYNC_SESSION = aiohttp.ClientSession()

    async def asyncTearDown(self):
        await self.ASYNC_SESSION.close()

    async def test_async_download_return_type(self):
        """
        Asynchronous download.
        """
        df = await apq.async_download(
            self.ASYNC_SESSION,
            self.DATASET,
            self.YEAR,
            variables = self.VARIABLES,
            state = CA
        )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual([self.CONTENT[1]], df.values.tolist())





if __name__ == '__main__':
    unittest.main()