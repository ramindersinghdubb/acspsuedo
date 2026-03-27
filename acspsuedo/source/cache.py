"""
Cache handler for variable/table metadata.
"""
import typing as t
import itertools as it
from collections import defaultdict
from logging import getLogger
from warnings import warn

import pandas as pd

from acspsuedo.source.low.exceptions import APIException
from acspsuedo.source.low.var_fetch import VariableFetchMixin



logger = getLogger(__name__)


class MetadataWarning(UserWarning):
    """Custom warning class for metadata issues."""
    pass


class VariableCache:
    """
    Class for caching metadata on variables and tables in American
    Community Survey datasets.
    """
    __CACHE_VARIABLES_BY_DATASET_YEAR: t.DefaultDict[
        str, t.DefaultDict[int, dict[str, t.Any]]
    ] = defaultdict(lambda: defaultdict(dict))

    __CACHE_TABLES_BY_DATASET_YEAR: t.DefaultDict[
        str, t.DefaultDict[int, dict[str, dict]]
    ] = defaultdict(lambda: defaultdict(dict))


    def __init__(self, cache_metadata_dfs: bool = True) -> None:
        """
        Initialization for :py:class:`VariableCache`.

        Parameters
        ----------
        cache_metadata_dfs
            Boolean; default True.
            
            Indicate whether or not to locally cache any created instances
            of the :py:class:`pandas.DataFrame` for querying metadata.
        """
        
        self._var_fetch = VariableFetchMixin()
        self._cache_metadata_dfs = cache_metadata_dfs
        
        if self._cache_metadata_dfs:
            self._CACHE_VAR_DF_BY_DATASET_YEAR: t.DefaultDict[
                str, dict[int, pd.DataFrame]
            ] = defaultdict(dict)

    @property
    def cache_metadata_dfs(self):
        """
        The state of locally caching any created :py:class:`pandas.DataFrame`
        instances containing variable metadata. Default True.
        """
        return self._cache_metadata_dfs
    
    @cache_metadata_dfs.setter
    def cache_metadata_dfs(self, new_cache_state: bool):
        self._cache_metadata_dfs = new_cache_state

    
    def get_table(self, dataset: str, year: int, table: str):
        """
        Get metadata information for all variables in a table.

        Parameters
        ----------
        dataset
            The dataset of interest.

        year
            A calendar year of interest. Note that the dataset must be available
            for this year.

        table
            A supported table within the dataset of interest.
        """
        try:
            metadata = VariableCache.__CACHE_TABLES_BY_DATASET_YEAR[dataset][year][table]
        except:
            metadata = self.fetch_table(dataset, year, table)
            logger.debug(
                "Did not find table '%s' for the '%s' dataset during the %s calendar year in cache. "
                "Fetched and cached for potential re-use later", table, dataset, year
            )

        return metadata


    def get_variable(self, dataset: str, year: int, variable: str):
        """
        Get metadata information for a particular variable.

        Parameters
        ----------
        dataset
            The dataset of interest.

        year
            A calendar year of interest. Note that the dataset must be available
            for this year.

        variable
            A supported variable within the dataset of interest.
        """
        try:
            metadata = VariableCache.__CACHE_VARIABLES_BY_DATASET_YEAR[dataset][year][variable]
        except KeyError:
            metadata = self._fetch_variable(dataset, year, variable)
            logger.debug(
                "Did not find the '%s' variable for the '%s' dataset during the %s calendar year in cache. "
                "Fetched and cached for potential re-use later", variable, dataset, year
            )

        return metadata
    
    def fetch_table(self, dataset: str, year: int, table: str, drop_annotation_vars: bool = True):
        """
        Fetch a table from the dataset for the specified calendar year.

        Parameters
        ----------
        dataset
            The dataset of interest.

        year
            A calendar year of interest. Note that the dataset must be available
            for this year.

        table
            A supported table within the dataset of interest.

        drop_annotation_vars
            Boolean. Default True.

            Indicate whether or not to show accompanying annotation variables.
        """
        json_content = self._var_fetch._fetch_table_json_content(dataset, year, table)

        all_variables = json_content.get("variables", None)
        if not all_variables:
            warn(
                f"\nThe '{table}' table has no known metadata for the '{dataset}' dataset \n"\
                f"during the {year} calendar year.",
                MetadataWarning
            )
            return
        
        if drop_annotation_vars:
            all_variables = {k: v for k, v in all_variables.items() if not k.endswith('A')}

        tbl = {table: all_variables}
        
        # We always want to cache
        for var_name, var_info in all_variables.items():
            VariableCache.__CACHE_VARIABLES_BY_DATASET_YEAR[dataset][year][var_name] = var_info

        for tbl_name, tbl_info in tbl.items():
            VariableCache.__CACHE_TABLES_BY_DATASET_YEAR[dataset][year][tbl_name] = tbl_info

        return tbl
    
    def fetch_all_tables(self, dataset: str, year: int):
        """
        Fetch all tables from the dataset for the specified calendar year.
        
        Parameters
        ----------
        dataset
            The dataset of interest.

        year
            A calendar year of interest. Note that the dataset must be available
            for this year.
        """
        json_content = self._var_fetch._fetch_table_json_content(dataset, year)

        # Fail-fast for empty datasets.
        all_grps = json_content.get("groups", None)
        if not all_grps:
            raise APIException(
                f"Found no tables for the '{dataset}' during the calendar year {year}."
            )

        grp_names = [grp.get("name") for grp in all_grps]
        
        # Fast approach
        # As opposed to querying each individual table from the API,
        # we get all variables directly and sort by group accordingly.
        content = self._fetch_variable(dataset, year)

        grps_dict: t.DefaultDict[
            str, dict
        ] = defaultdict(dict)
        for var_name, var_info in content.items():
            grp_name = var_info.get('group')
            if grp_name in grp_names:
                grps_dict[grp_name].update({var_name: var_info})

        # We always want to cache
        for tbl_name, tbl_info in grps_dict.items():
                VariableCache.__CACHE_TABLES_BY_DATASET_YEAR[dataset][year][tbl_name] = tbl_info
        
        return grps_dict

    
    def _fetch_variable(self, dataset: str, year: int, variable: t.Optional[str] = None):
        """
        Fetch a single variable, or all variables, from a dataset.
        """
        json_content = self._var_fetch._fetch_json_content(dataset, year, variable)
        
        all_variables = json_content.get("variables", None)

        if all_variables:
            # Drop 'for', 'in', 'ucgid'
            metadata_dict = { k:v for k, v in all_variables.items()
                             if k not in ["for", "in", "ucgid"] }
            
            # MOE estimate metadata is not directly provided for the variables metadata
            # (but IS available for table metadata; strange!), so we have to manually
            # update accordingly.
            metadata_dict = {
                **metadata_dict,
                **{k.removesuffix('E') + 'M':
                   {**v, 'label': v.get('label').replace('Estimate', 'Margin of Error', 1)}
                for k, v in metadata_dict.items() if k.endswith('E')}
            }

        else:
            name = json_content.pop('name')
            metadata_dict = {name: json_content}
        
        # We always cache
        for var_name, var_info in metadata_dict.items():
            VariableCache.__CACHE_VARIABLES_BY_DATASET_YEAR[dataset][year][var_name] = var_info
        
        return metadata_dict
    
    def var_metadata_df(self, dataset: str, year: int):
        """
        Return a :py:class:`pandas.DataFrame` containing holistic metadata for all
        variables in a dataset for a given year.

        Parameters
        ----------
        dataset
            The dataset of interest.

        year
            A calendar year of interest. Note that the dataset must be available
            for this year.
        """
        if self._cache_metadata_dfs:
            try:
                df = self._CACHE_VAR_DF_BY_DATASET_YEAR[dataset][year]
                return df
            except:
                pass

        json_content = self._fetch_variable(dataset, year)

        df = pd.DataFrame([
            {
                'DATASET': dataset,
                'YEAR': year,
                'VARIABLE': var_name,
                'LABEL': var_info.get('label'),
                'VARIABLE_TYPE': var_info.get('predicateType', 'string'),
                'TABLE': var_info.get('group'),
                'TOPIC': var_info.get('concept', '').title(),
                'RELATED_ATTRIBUTES': var_info.get('attributes'),
                'LIMIT': var_info.get('limit'),
            }
            for var_name, var_info in sorted(json_content.items())
        ])

        if self._cache_metadata_dfs:
            self._CACHE_VAR_DF_BY_DATASET_YEAR[dataset][year] = df

        return df
    
    def tbl_metadata_df(self, dataset: str, year: int, table: str, drop_annotation_vars: bool = True):
        """
        Return a :py:class:`pandas.DataFrame` containing holistic metadata for
        all variables in a dataset table for a given year.

        Parameters
        ----------
        dataset
            The dataset of interest.

        year
            A calendar year of interest. Note that the dataset must be available
            for this year.

        table
            The table of interest. Note that the dataset must contain this table.

        drop_annotation_vars
            Boolean; default True.

            Indicate whether or not to drop any accompanying annotation variables.
        """
        tbl = self.fetch_table(dataset, year, table, drop_annotation_vars)

        if tbl:
            tbl_vars = tbl.get(table, None)

            if tbl_vars:
                for i in ['GEO_ID', 'NAME']:
                    tbl_vars.pop(i, None)
                df = pd.DataFrame([
                    {
                        'DATASET': dataset,
                        'YEAR': year,
                        'VARIABLE': var_name,
                        'LABEL': var_info.get('label'),
                        'VARIABLE_TYPE': var_info.get('predicateType', 'string'),
                        'TABLE': var_info.get('group'),
                        'TOPIC': var_info.get('concept', '').title(),
                        'LIMIT': var_info.get('limit'),
                    }
                    for var_name, var_info in sorted(tbl_vars.items())
                ])

                return df
        
        raise APIException(
            f"The '{table}' table was empty and/or non-existent for the '{dataset}' dataset "
            f"for the {year} calendar year."
        )
    

    def _tbl_var_list(
        self,
        dataset: str,
        year: int,
        vars: t.Optional[t.Union[t.List[str], str]] = None,
        tbls: t.Optional[t.Union[t.List[str], str]] = None,
    ) -> t.Tuple[t.List[t.Any], t.Dict[t.Any, t.Any]]:
        """
        Create a list of variables from the supplied variable(s) and table(s), as well as
        a metadata dictionary containing the data types for each variable. The upside of
        this particular approach is to provide a simpler end-consumption for querying
        multiple variables of interest, especially if some share table location, and to
        provide metadata on each variable's typing.

        Parameters
        ----------
        dataset
            A dataset of interest

        year
            A calendar year of interest. Dataset must be supported for this year.

        vars
            One, none, or multiple variables. Default None.

        tbls
            One, none, or multiple tables. Default None.

        Returns
        -------
        A tuple containing:
        - All variables from the specification provided
        - Each of the variables corresponding data types.
        """
        var_meta_df = self.var_metadata_df(dataset, year)

        if not isinstance(vars, list):
            vars = [vars]
        if not isinstance(tbls, list):
            tbls = [tbls]

        
        # Collect all variable information, from the specified
        # variables and those found in the specified tables
        tbl_vars = []
        if tbls:
            tbl_vars = list(
                it.chain(
                    *[list(var_meta_df['VARIABLE'][var_meta_df['TABLE'] == tbl]) for tbl in tbls]
                )
            )
        vars.extend(tbl_vars)

        all_vars = [var for var in vars if var is not None]

        # Check if any of the supplied vars do not exist or
        # empty variables.
        errors = [var for var in all_vars if var not in list(var_meta_df['VARIABLE'])]
        if errors:
            raise APIException(
                f"Found at least one variable that was unrecognizable from the querying "
                f"information supplied: {errors}."
            )
        if not all_vars:
            raise APIException(
                "Non-existent variables; the specified tables and/or variables do not "
                f"exist for the '{dataset}' dataset during the {year} calendar year."
            )
        
        var_df = var_meta_df[var_meta_df['VARIABLE'].str.contains('|'.join(all_vars))]
        meta_dict = dict(zip(var_df['VARIABLE'], var_df['VARIABLE_TYPE']))

        return all_vars, meta_dict
    

    def _set_dtypes(
        self,
        data_df: pd.DataFrame,
        meta_dict: t.Dict[t.Any, t.Any]
    ):
        """
        Convert the data types for the fetched census data.

        Parameters
        ----------
        data_df
            The fetched data from the Census Bureau.

        meta_dict
            A dictionary specifiying dtypes for the fetched data.
        """
        # Type conversion
        for var, var_type in meta_dict.items():
            
            if var_type == 'int':
                try:
                    data_df[var] = data_df[var].astype(int)
                except:
                    data_df[var] = data_df[var].astype(float)
            
            elif var_type == 'float':
                data_df[var] = data_df[var].astype(float)
            
            elif var_type == 'string':
                data_df[var] = data_df[var].astype(str)
            
            # Last ditch attempt
            else:
                data_df[var] = data_df[var].astype(object)

        return data_df