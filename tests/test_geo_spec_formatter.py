"""
Tests for the geographic specifier formatter interface.
"""

import unittest

from acspsuedo.source.geog import GeoSpecFmtter, GeoScopeException
from acspsuedo.source.low.exceptions import APIException
from acspsuedo.datasets import ACS1, ACS5
from acspsuedo.fips.states import MONTANA
from acspsuedo.fips.counties.montana import Lewis_And_Clark_County



class TestGeoSpecFmtter(unittest.TestCase):
    
    def setUp(self) -> None:
        self.GSF_INTERFACE = GeoSpecFmtter

        self.DATASET = ACS1
        self.YEAR    = 2014

        self.PANDEMIC_YEAR = 2020

        self.GEO_SPECS_MONTANA_COUNTIES = {'state': MONTANA, 'county': '*'}
        self.GEO_SPECS_MONTANA_COUNTIES_FMTTER = '&for=county:*&in=state:30'

        self.GEO_SPECS_LEWIS_AND_CLARK_COUNTY = {'state': MONTANA, 'county': Lewis_And_Clark_County}
        self.GEO_SPECS_LEWIS_AND_CLARK_COUNTY_FMTTER = '&for=county:049&in=state:30'

        self.GEO_SPECS_NOT_SUPPORTED = ['state', 'block_group']

        self.GEO_SPECS_UNSUPPORTED_WC = {'state': '*', 'school_district_unified': '*'}

    def test_interface_instance(self):
        """
        Check the dunder methods for instances of the interface.
        """
        gsf_obj = self.GSF_INTERFACE(state = '53', place = '63000')
        self.assertTrue(len(gsf_obj) == 2)

        self.assertEqual(str(gsf_obj), "GeoSpecFmtter(state = '53', place = '63000')")

        self.assertIsInstance(gsf_obj(self.DATASET, self.YEAR), str)

        self.assertEqual(gsf_obj.geog_specifiers, {'place': '63000', 'state': '53'})

        gsf_obj.geog_specifiers = {'state': '53'}

    def test_check_path_existence(self):
        """
        Check if our path existence locator works.
        """
        self.GSF_INTERFACE.check_path_existence(
            self.DATASET,
            self.YEAR,
            'state'
        )

    def test_geo_spec_formatter_montana_counties_acs1_2014(self):
        """
        Formatter for Montana counties, 2014.
        """
        fmtter, _ = self.GSF_INTERFACE.get_fmt_path(
            self.DATASET,
            self.YEAR,
            **self.GEO_SPECS_MONTANA_COUNTIES
        )
        
        self.assertEqual(fmtter, self.GEO_SPECS_MONTANA_COUNTIES_FMTTER)

    def test_geo_spec_formatter_lewis_and_clark_county_acs1_2014(self):
        """
        Formatter for Lewis and Clark county, Montana, 2014.
        """
        fmtter, _ = self.GSF_INTERFACE.get_fmt_path(
            self.DATASET,
            self.YEAR,
            **self.GEO_SPECS_LEWIS_AND_CLARK_COUNTY
        )
        
        self.assertEqual(fmtter, self.GEO_SPECS_LEWIS_AND_CLARK_COUNTY_FMTTER)

    def test_partial_path_inference_acs5_2009(self):
        """
        Check if the default to partial inference works.
        """
        with self.assertRaises(GeoScopeException):
            self.GSF_INTERFACE.get_fmt_path(
                ACS5, 2009, block_group = '*', foo = 'bar'
            )

        with self.assertRaises(GeoScopeException):
            self.GSF_INTERFACE._infer_path(
                ACS5, 2009, school_district_elementary = '*'
            )

    def test_unsupported_year_acs1_2020_2004(self):
        """
        Check if an error is raised for unsupported years.
        """
        with self.assertRaises(APIException):
            self.GSF_INTERFACE.get_fmt_path(
                self.DATASET,
                self.PANDEMIC_YEAR,
                **self.GEO_SPECS_MONTANA_COUNTIES
            )

        with self.assertRaises(APIException):
            self.GSF_INTERFACE.get_fmt_path(
                self.DATASET,
                2004,
                **self.GEO_SPECS_MONTANA_COUNTIES
            )

    def test_unsupported_dataset(self):
        """
        Check if an error is raised for an unsupported dataset.
        """
        with self.assertRaises(KeyError):
            self.GSF_INTERFACE.get_fmt_path(
                'foo/bar',
                self.YEAR,
                **self.GEO_SPECS_MONTANA_COUNTIES
            )

    def test_cache_acs1_2014(self):
        """
        Check if our paths are represented as a list of lists.
        """
        paths = self.GSF_INTERFACE.view_geographic_paths(self.DATASET, self.YEAR)

        self.assertTrue(paths, list)
        self.assertTrue(all(isinstance(path, list) for path in paths))

    def test_unsupported_specifiers_acs1_2014(self):
        """
        Check if an unsupported set of geographic specifiers raises a warning.
        """
        with self.assertWarns(UserWarning):
            self.GSF_INTERFACE.check_path_existence(
                self.DATASET,
                self.YEAR,
                self.GEO_SPECS_NOT_SUPPORTED
            )

    def test_unsupported_wildcard_specifier_acs1_2014(self):
        """
        Check if the interface raises an error for specifiers that are
        accidentally supplied with a wildcard operator.
        """
        with self.assertRaises(GeoScopeException):
            self.GSF_INTERFACE._infer_path(
                self.DATASET,
                self.YEAR,
                **self.GEO_SPECS_UNSUPPORTED_WC
            )






if __name__ == '__main__':
    unittest.main()