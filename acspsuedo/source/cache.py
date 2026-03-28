"""
Cache handler for variable/table metadata.
"""
import typing as t
from collections import defaultdict, ChainMap
from logging import getLogger
from warnings import warn

import pandas as pd

from acspsuedo.source.low.exceptions import APIException
from acspsuedo.source.low.var_fetch import VariableFetchMixin


logger = getLogger(__name__)


class MetadataException(APIException):
    """Custom exception class for metadata issues."""
    pass


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
            metadata = self._fetch_table(dataset, year, table)[table]
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
        except:
            try:
                # Load everything and see if it exists
                metadata = self._fetch_variable(dataset, year, None)[variable]
            except:
                # Fall back to here
                # This is usually in the case of users wishing to specify idiosyncratic annotation
                # attribute data, since they aren't in the full collection of variables.
                metadata = self._fetch_variable(dataset, year, variable)[variable]
            logger.debug(
                "Did not find the '%s' variable for the '%s' dataset during the %s calendar year in cache. "
                "Fetched and cached for potential re-use later", variable, dataset, year
            )

        return metadata
    
    def _fetch_table(self, dataset: str, year: int, table: str):
        """
        Fetch a table from the dataset for the specified calendar year.
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

        tbl = {table: all_variables}
        
        # We always want to cache
        VariableCache.__CACHE_TABLES_BY_DATASET_YEAR[dataset][year][table] = all_variables

        for var_name, var_info in all_variables.items():
            VariableCache.__CACHE_VARIABLES_BY_DATASET_YEAR[dataset][year][var_name] = var_info

        return tbl
    
    def _fetch_all_tables(self, dataset: str, year: int):
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
        if all_grps is None:
            raise APIException(
                f"Found no tables for the '{dataset}' during the calendar year {year}."
            )

        grp_names = [grp.get("name") for grp in all_grps]
        
        # Fast approach
        # As opposed to querying each individual table from the API,
        # we get all variables directly and sort by group accordingly.
        content = self._fetch_variable(dataset, year)

        grps_dict = defaultdict(dict)
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
            }
            for var_name, var_info in sorted(json_content.items())
        ])

        if self._cache_metadata_dfs:
            self._CACHE_VAR_DF_BY_DATASET_YEAR[dataset][year] = df

        return df
    
    def tbl_metadata_df(self, dataset: str, year: int, table: str):
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
        """
        tbl_vars = self.get_table(dataset, year, table)

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
                }
                for var_name, var_info in sorted(tbl_vars.items())
            ])

            return df
        
        raise APIException(
            f"The '{table}' table was empty and/or non-existent for the '{dataset}' dataset "
            f"for the {year} calendar year."
        )
    

    def _vars_metadata(
        self,
        dataset: str,
        year: int,
        vars: t.Optional[t.Union[t.List[str], str]] = None,
        tbls: t.Optional[t.Union[t.List[str], str]] = None,
        drop_annotation_vars: bool = True
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

        drop_annotation_vars
            Boolean; default True. Indicate whether or not to drop supplementary
            attribute/annotation/margin-of-error variables.

        Returns
        -------
        A tuple containing:
        - All variables from the specification provided
        - Each of the variables corresponding data types.
        """

        if not isinstance(vars, list):
            vars = [vars]
        if not isinstance(tbls, list):
            tbls = [tbls]

        vars = [var for var in vars if var is not None]
        tbls = [tbl for tbl in tbls if tbl is not None]

        metadata = []

        if vars:
            for var in vars:
                try:
                    var_dict = self.get_variable(dataset, year, var)
                    metadata.append({var: var_dict})
                except APIException:
                    raise MetadataException(
                        f"The '{var}' variable was not recognized for the '{dataset}' dataset.",
                    ) from None
        if tbls:
            for tbl in tbls:
                try:
                    tbl_dict = self.get_table(dataset, year, tbl)
                    metadata.append(tbl_dict)
                except APIException:
                    raise MetadataException(
                        f"The '{tbl}' table was not recognized for the '{dataset}' dataset.",
                    ) from None

        # Parse out annotation and MOE variables, if indicated
        metadata = dict(ChainMap(*metadata))
        if drop_annotation_vars:
            metadata = {k: v for k,v in metadata.items() if not
                        any(x in v.get('label', '') for x in ['Annotation', 'Margin of Error'])}
            
        if not metadata:
            raise APIException(
                "Non-existent variables; the specified tables and/or variables do not "
                f"exist for the '{dataset}' dataset during the {year} calendar year."
            )
        
        meta_dict = {k: v.get('predicateType', '') for k, v in metadata.items()}
        all_vars = list(meta_dict.keys())

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
    

    @classmethod
    def _flush_var_metadata_cache(cls, dataset: str, year: int):
        VariableCache.__CACHE_VARIABLES_BY_DATASET_YEAR[dataset][year].clear()

    @classmethod
    def _flush_tbl_metadata_cache(cls, dataset: str, year: int):
        VariableCache.__CACHE_TABLES_BY_DATASET_YEAR[dataset][year].clear()