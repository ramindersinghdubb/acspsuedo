"""
Fetch handler for variable/table metadata.
"""

import typing as t

from acspsuedo.source.low.protocols import fetch_content
from acspsuedo.source.low.api_key import api_key_config



class VariableFetchMixin:
    """
    Mix-in to handle fetching metadata on variables and tables
    in ACS datasets.
    """
    @staticmethod
    def _fetch_json_content(dataset: str, year: int, variable: t.Optional[str] = None):
        """
        Fetch the JSON content for a dataset variable.
        """
        url = VariableFetchMixin.dataset_variables_url(dataset, year, variable)
        json_content = fetch_content(url)
        
        return json_content

    @staticmethod
    def _fetch_table_json_content(dataset: str, year: int, table: t.Optional[str] = None):
        """
        Fetch the JSON content for a dataset table.
        """
        url = VariableFetchMixin.dataset_tables_url(dataset, year, table)
        json_content = fetch_content(url)
        
        return json_content
    
   
    @staticmethod
    def dataset_variables_url(dataset: str, year: int, variable: t.Optional[str] = None):
        """
        URL constructor to fetch a variable's metadata.
        """
        variable = f'/{variable}' if variable else ''
        url = f'{VariableFetchMixin._base_url_comp}/{VariableFetchMixin._dataset_year_url_comp(dataset, year)}/variables{variable}.json?{api_key_config._get_api_key()}'
        
        return url

    @staticmethod
    def dataset_tables_url(dataset: str, year: int, table: t.Optional[str] = None):
        """
        URL constructor to fetch a table's metadata.
        """
        table = f'/{table}' if table else ''
        url = f'{VariableFetchMixin._base_url_comp}/{VariableFetchMixin._dataset_year_url_comp(dataset, year)}/groups{table}.json?{api_key_config._get_api_key()}'
        
        return url
    
    _base_url_comp = 'https://api.census.gov/data'
    
    @classmethod
    def _dataset_year_url_comp(cls, dataset: str, year) -> str:
        return f'{year}/{dataset}'