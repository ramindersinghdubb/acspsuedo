"""
Tests for the cache interface.
"""

import unittest

from acspsuedo.source.cache import VariableCache, MetadataException
from acspsuedo.source.low.exceptions import APIException
from acspsuedo.datasets import ACS3



class TestVariableCache(unittest.TestCase):

    def setUp(self) -> None:
        self.DATASET = ACS3
        self.VCI = VariableCache()

        self.MEDIAN_INCOME_BY_NATIVITY_VAR = 'B06011_001E'
        self.TOTAL_TRAVELING_TO_WORK_BY_AUTOMOBILE_VAR = 'C08301_002E'
        self.VARS = ['B06011_001E', 'C08301_002E']

        self.HEALTH_INSURANCE_COVERAGE_TWO_OR_MORE_RACES_TBL = 'B27001G'
        self.TRAVEL_TIME_TO_WORK_TBL = 'C08303'
        self.TBLS = ['B27001G', 'C08303']

    def test_interface_instance(self):
        """
        Simulate a case where the user introspects the cache interface.
        """
        cache_obj = VariableCache(False)

        self.assertFalse(cache_obj.cache_metadata_dfs)

        cache_obj.cache_metadata_dfs = True

        self.assertTrue(cache_obj.cache_metadata_dfs)

    def test_fetch_variable_metadata_df(self):
        """
        Simulate a case where a user queries a metadata on all variables.
        """
        self.VCI.var_metadata_df(self.DATASET, 2011)

    def test_fetch_table_metadata_df(self):
        """
        Simulate a case where a user queries a singular table.
        """
        self.VCI.tbl_metadata_df(self.DATASET, 2011, 'B17015')

    def test_fetch_non_existent_table_metadata_df(self):
        """
        Simulate a case where a user queries a non-existent table.
        """
        with self.assertRaises(APIException):
            self.VCI.tbl_metadata_df(self.DATASET, 2011, 'foo bar')

    def test_fetch_all_tables_metadata(self):
        """
        Simulate a case of querying all tables.
        """
        self.VCI._fetch_all_tables(self.DATASET, 2011)


    def test_fetch_table_metadata_df_in_cache(self):
        """
        Simulate a case of fetching a cached table a non-existent singular table.
        """
        self.VCI.tbl_metadata_df(self.DATASET, 2011, 'B17016')
        self.VCI.tbl_metadata_df(self.DATASET, 2011, 'B17016')

    def test_fetch_table_metadata_df_failed(self):
        """
        Simulate a case of querying a non-existent singular table.
        """
        with self.assertRaises(APIException):
            self.VCI.tbl_metadata_df(self.DATASET, 2011, 'hello world')

    def test_fetch_vars_fmtter(self):
        """
        Simulate the variable-fetch formatter.
        """
        self.VCI._vars_metadata(
            self.DATASET,
            2011,
            self.MEDIAN_INCOME_BY_NATIVITY_VAR,
            self.HEALTH_INSURANCE_COVERAGE_TWO_OR_MORE_RACES_TBL
        )

        self.VCI._vars_metadata(
            self.DATASET,
            2011,
            self.VARS,
            self.TBLS
        )

    def test_fetch_vars_fmtter_failed(self):
        """
        Simulate a failed variable-fetch formatter.
        """
        FOO = 'foo'
        BAR = 'bar'

        # Non-existent variable
        with self.assertRaises(MetadataException):
            self.VCI._vars_metadata(
                self.DATASET,
                2011,
                FOO,
            )

        # Non-existent table
        with self.assertRaises(MetadataException):
            self.VCI._vars_metadata(
                self.DATASET,
                2011,
                self.MEDIAN_INCOME_BY_NATIVITY_VAR,
                BAR
            )

        # Lack of variables and tables
        with self.assertRaises(APIException):
            self.VCI._vars_metadata(
                self.DATASET,
                2011
            )



if __name__ == '__main__':
    unittest.main()