"""
Handler objects for geographic specifiers and the (optionally
user supplied) Census Bureau API key.
"""
import os
import typing as t
import collections.abc as cABC
from itertools import groupby, combinations
from collections import namedtuple, defaultdict
from pathlib import Path
from warnings import warn
from logging import getLogger


from acspsuedo.source.low.protocols import fetch_content
from acspsuedo.source.low.exceptions import APIException
from acspsuedo.source.shpfile_fmt import GEO_SPEC_METADATA
from acspsuedo.datasets import API_METADATA


logger = getLogger(__name__)


class GeoScopeException(APIException):
    """Exceptions for geographic scope-related errors."""
    pass

class UnsupportedSpecException(APIException):
    """Exceptions for any unsupported specifier handling."""
    pass



class GeoSpecFmtter:
    """
    Formatter for geographic specifiers.
    """
    def __init__(
        self,
        **geog_specifiers
    ) -> None:
        """
        Formatter for geographic specifiers.

        Note that instances can double as callables, such that the results of the
        callable refer to the fully-specified geographic path (for a dataset of
        interest during a calendar year) inferred from the supplied specifiers.
        
        Parameters
        ----------
        geog_specifiers
            Geographic specifiers of interest.
            
            See `GeoSpecFmtter.view_geographic_areas()` to view an exhaustive list
            of fully-specified paths, each containing the component geographic
            specifiers required for each path, or `GeoSpecFmtter.check_path_existence()`,
            to view whether or not a particular specifier (or specified path) is
            supported by a dataset of interest during a particular calendar year.
        """

        self._geog_specifiers = geog_specifiers

    @property
    def geog_specifiers(self):
        return self._geog_specifiers
    
    @geog_specifiers.setter
    def geog_specifiers(self, new_geog_specifiers):
        self._geog_specifiers = new_geog_specifiers

    def __len__(self):
        """Return the length of the supplied geographic specifiers."""
        return len(self.geog_specifiers)
    
    def _kwarg_fmt(self):
        return ', '.join( [f"{k} = '{v}'" for k, v in self.geog_specifiers.items()] )
    
    def __str__(self) -> str:
        return f"GeoSpecFmtter({self._kwarg_fmt()})"

    def __repr__(self) -> str:
        return str(self)
    
    def __call__(self, dataset: str, year: int) -> str:
        """
        Infer the specific path for a given dataset on a supported year
        using the instance's supplied keyword arguments and return the
        formatted geographic path.
        """
        return GeoSpecFmtter.get_fmt_path(dataset, year, **self.geog_specifiers)
    
    @classmethod
    def check_path_existence(
        cls,
        dataset: str,
        year: int,
        geographic_specifier: t.Union[cABC.Iterable[str], str]
    ) -> t.Optional[list[list[str]]]:
        """
        Check if the specifier, or collection of specifiers, is supported for the
        dataset of interest during the specified calendar year.

        If supported, returns a list of fully-specified geographic paths containing
        at least one of the supplied specifiers anywhere.

        Parameters
        ----------
        dataset
            The Census Bureau dataset of interest. See `~datasets`
            for a list of supported datasets.

        year
            The calendar year, for which the dataset must be supported
            on.

        geographic_specifier
            One, or multiple, geographic specifiers.


        Returns
        -------
        If found, returns a list containing all fully-specified paths corresponding
        to the specifier or collection of specifiers. Otherwise, a warning is raised.
        """
        
        if isinstance(geographic_specifier, str):
            geographic_specifier = [geographic_specifier]

        paths = cls.view_geographic_paths(dataset, year)
        paths = [p for p in paths if all(q in p for q in geographic_specifier)]
        
        if paths:
            return paths
        else:
            msg = \
            f"\nCould not find any fully-specified paths corresponding to " \
            f"{geographic_specifier} in the '{dataset}' dataset for the calendar\n" \
            f"year {year}. This may be due to a combination of the following reasons:\n" \
            "   1. Potential misspelling in the geographic specifier(s)\n" \
            "   2. Unavailable and/or unsupported geographic specifier(s) for the dataset and calendar year."

            warn( msg, UserWarning )
            return
    
    @classmethod
    def view_geographic_paths(
        cls,
        dataset: str,
        year: int
    ) -> list[list[str]]:
        """
        View all fully-specified geographic paths that are
        supported by a dataset of interest during the calendar
        year of interest.

        Parameters
        ----------
        dataset
            The Census Bureau dataset of interest. See `~datasets`
            for a list of supported datasets.

        year
            The calendar year, for which the dataset must be supported
            on.

        Returns
        -------
        A list of fully-specified geographic pathways, each of which
        are a list and whose respective last element represent the
        'for' clause in queries to the Census Bureau.
        
        For instance, one fully-specified path may be ['state', 'county']
        where 'county' represents the 'for' clause to specify queries to
        a particular geographic scope while 'state' represents the 'in'
        clause, which governs how geographies to the aforementioned scope
        are restricted to.
        
        Thus, for this particular example, data will be shown for county-level
        geographies based on the restriction in the specified state-level
        geographies: `{'state': '06', 'county': '*'}` specifies the user wishes
        to query all county-level geographies (the wildcard '`*`' operator) for
        the state of California ('06' corresponding to the FIPS code for California).
        """
        
        paths = cls.__get_all_paths(dataset, year)
        
        paths = [list(p.kwargs) for p in paths]

        return paths
    
    @classmethod
    def get_geo_cols(cls, **geog_specifiers):
        """
        Given a set of geographic specifiers, list the names of their respective
        geographic columns that will be returned from data queries.
        """
        return [x for k in geog_specifiers for x in GEO_SPEC_METADATA[k][1]]
    
    _repl: t.Callable[[str], str] = lambda x: x.replace('(', '') \
        .replace(')', '') \
        .replace('-', '_') \
        .replace('/', '_') \
        .replace(' ', '_')
    
    __CACHE_PATHS_BY_DATASET_YEAR: t.DefaultDict[
        str, dict[int, list['_InferSpec']]
    ] = defaultdict(dict)
    """
    Internal for caching information on geographic pathways w/o having
    to query the Bureau's API each time.
    """

    @classmethod
    def get_fmt_path(
        cls,
        dataset: str,
        year: int,
        **kwargs
    ) -> str:
        """
        Given a set of geographic specifiers, get the fully-specified
        geographic path for a dataset of interest on a supported year.

        Parameters
        ----------
        dataset
            The Census Bureau dataset of interest. See `~datasets`
            for a list of supported datasets.

        year
            The calendar year, for which the dataset must be supported
            on.

        **kwargs
            A set of geographic specifiers. Note that the wildcard
            operator, `*`, indicates that the user wishes to query
            information for the entire set of geographies of the
            scope (e.g. `state = '*'` indicates the users wishes to
            view a dataset's information across all states).

        Returns
        -------
        A formatted string corresponding to the fully specified
        geographic path.
        """
        return cls._fmt_path(dataset, year, **kwargs)
        
    
    @classmethod
    def _fmt_path(
        cls,
        dataset: str,
        year: int,
        **kwargs
    ):
        """
        Internal for taking the inferred geographic path
        and formatting it into the data query URL.
        """
        path = cls._infer_path(dataset, year, **kwargs)
        logger.debug('Inferred the following path -- %s', path)
        
        if path.len == 1:
            return '&for={}:{}'.format(*path.spec, *path.kwargs.values())
        else:
            *in_k, for_k = path.spec
            *in_v, for_v = path.kwargs.values()
            
            in_clause = ' '.join([f'{k}:{v}' for k, v in zip(in_k, in_v)])
            
            return '&for={}:{}&in={}'.format(for_k, for_v, in_clause)

    @classmethod
    def _infer_path(
        cls,
        dataset: str,
        year: int,
        **kwargs
    ) -> "_InferSpec":
        """
        Given a set of geographic specifiers, infer the path
        specification of interest for a dataset on a given year.

        Parameters
        ----------
        dataset
            The Census Bureau dataset of interest. See `~datasets`
            for a list of supported datasets.

        year
            The calendar year, for which the dataset must be supported
            on.

        **kwargs
            A set of geographic specifiers. Note that the wildcard
            operator, `*`, indicates that the user wishes to query
            information for the entire set of geographies of the
            scope (e.g. `state = '*'` indicates the users wishes to
            view a dataset's information across all states).

        Returns
        -------
        An internal (:py:class:`~_InferSpec`, subclassing
        `collections.namedtuple`) for formatting query URLs.
        """
        all_paths = cls.__get_all_paths(dataset, year)

        path = cls.__infer_path(dataset, year, all_paths, **kwargs)
        
        cls.__wc_check(path)    

        return path
    
    @classmethod
    def __get_all_paths(
        cls,
        dataset: str,
        year: int
    ) -> list['_InferSpec']:
        """
        Fetch protocol to get all of a dataset's geographic paths for
        a particular year.

        If a dataset for a certain year has already been fetched in some
        way (whether that be perusing, or path inference), it will be
        cached so as to avoid repeated calls to the Bureau.
        """
        try:
            paths = cls.__CACHE_PATHS_BY_DATASET_YEAR[dataset][year]
            logger.debug("Found the '%s' dataset for the calendar year %s in cache.",dataset, year)
        except:
            paths = cls._list_geo_specs(dataset, year)
            cls.__CACHE_PATHS_BY_DATASET_YEAR[dataset][year] = paths
            logger.debug(
                "Could not find the '%s' dataset for the calendar year %s in cache. "
                "Fetched and cached for potential re-use later.", dataset, year
            )
        
        return paths
    
    @classmethod
    def __wc_check(
        cls,
        path: '_InferSpec',
    ) -> None:
        """
        Check if any specifiers are supplied with wildcards that shouldn't be.
        Raises an error.
        """
        wc_errors = [k for check, (k, v) in zip(path.spec, path.kwargs.items())
                     if v == '*' and not path.supports_wildcard[check]]
        
        if wc_errors:
            raise GeoScopeException(
                f"The wildcard operator (`*`) is not permitted for the specifier(s) {wc_errors} "
                f"in the supplied path: {path.spec}."
            )
        
    @classmethod
    def __infer_path(
        cls,
        dataset: str,
        year: int,
        geo_specs: list['_InferSpec'],
        **kwargs
    ):
        """
        Actual implementation of inferring a path specification.
        """
        
        path_len = len(kwargs)

        # Loose search for matches
        #
        # Note that while we loosely search for paths
        # containing at least one instance of any specifier,
        # we are also priming for irreducibility by testing
        # the length of supplied (and supported) specifiers
        # (cf. strict test below).
        opts = [p for p in geo_specs if any(k in p.kwargs for k in kwargs)]

        if opts:
            full = [p for p in opts if (p.len == path_len) and
                    all(k in p.kwargs for k in kwargs)]
            
            if full:
                return cls._full_match(full, **kwargs)
            else:
                # 'for' clause is governed by the last specifier of a path.
                avail_specs = list(dict.fromkeys([list(p.kwargs)[-1] for p in geo_specs]))

                return cls._partial_match(dataset, year, opts, avail_specs, **kwargs)
            
        else:
            raise GeoScopeException(
                "Invalid/unsupported geographic specifiers were supplied for the "
                f"'{dataset}' dataset for the calendar year {year}."
            )
    
    @classmethod
    def _full_match(
        cls,
        full: list['_InferSpec'],
        **kwargs
    ) -> '_InferSpec':
        """
        Full test.
        """
        # Note that, by this construction, each path is irreducible. Thus, if a path (e.g.
        # ['state', 'county']) was similar to that containing itself and some additional
        # specifiers (e.g. ['state', 'county', 'tract']), only the former would be matched
        # because it is of length 2 (or the exact amount of specifiers supplied).
        if len(full) > 1:
            raise GeoScopeException(
                "Excessive geographic specifiers; could not infer a(n) unique geographic "
                f"path. Inferred multiple paths: {[list(p.kwargs) for p in full]}."
            )
        
        match = full[0]
        match.kwargs.update(kwargs)
        return match
        
    @classmethod
    def _partial_match(
        cls,
        dataset: str,
        year: int,
        opts: list['_InferSpec'],
        avail_specs: list,
        **kwargs
    ):
        """
        Partial test. Defaulted to in the case of null full matches.
        """
        # Note that, in the initial construction, 2^n combinations are generated based on
        # the supported kwargs inputed, where n represents the length of supported kwargs.
        # Thus, we are effectively searching for spec paths such that they correspond to
        # at least one of these 2^n path combinations. From there, if the generated results
        # miss any of the specifiers, we go back and specify it.

        c_test = [c for L in range(len(kwargs) + 1) for c in combinations(kwargs, L) ]
        
        p_opts = [list(p.kwargs) for c in c_test for p in opts if set(p.kwargs).issubset(c)]

        # If we miss paths for certain specifiers, add them.
        for k in kwargs:
            if not any(k in i for i in p_opts):
                p_opts.extend([list(p.kwargs) for p in opts if k in p.kwargs])

        # Remove duplicates
        p_opts.sort()
        p_opts = list(p_opts for p_opts, _ in groupby(p_opts))

        # If any scopes are not supported, list them.
        unsupported = [k for k in kwargs if k not in avail_specs]
        usd_msg = f' Additionally, invalid/unsupported specifiers were found: {unsupported}.' if \
                unsupported else ''
        
        raise GeoScopeException(
            "Could not infer a fully-specified path from the supplied geographic specifiers: "
            f"{list(kwargs)}. Potential fully-specified path matches for the '{dataset}' "
            f"dataset during the calendar year {year} based on the supplied specifiers include "
            f"one of: {p_opts}.{usd_msg}"
        )


    @classmethod
    def _list_geo_specs(cls, dataset: str, year: int) -> list['_InferSpec']:
        """
        Internal for fetching all possible geographic paths for a given
        dataset during a given calendar year.
        """
        
        url = cls._url_fmt(dataset, year)
        content = fetch_content(url)
        geographies = content.get('fips', None)

        if geographies:
            geo_combinations = []

            for g in geographies:
                scope = g.get('name')
                reqs  = g.get('requires', [])
                opt_reqs = g.get('optionalWithWCFor', [])
                if not isinstance(opt_reqs, list):
                    opt_reqs = [opt_reqs]
                wcs = g.get('wildcard', [])

                d_scope, d_reqs = cls._repl(scope), map(cls._repl, reqs)

                scp = [*reqs, scope]
                d_scp = [*d_reqs, d_scope]

                wcs = {**{k: True if k in wcs else False for k in reqs}, scope: True}

                ifp = _InferSpec(scp, len(scp), dict.fromkeys(d_scp), wcs)
                geo_combinations.append(ifp)
                
                # Accomodate for any optional specifiers
                if opt_reqs:
                    for opt in opt_reqs:
                        mod_ifp = [j for j in scp if j != opt]
                        mod_wcs = {k: v for k, v in wcs.items() if k != opt}
                        d_mod_ifp = map(cls._repl, mod_ifp)
                        
                        mod_ifp = _InferSpec(mod_ifp, len(mod_ifp), dict.fromkeys(d_mod_ifp), mod_wcs)
                        geo_combinations.append(mod_ifp)

            return geo_combinations
        
        else:
            # Although American Community Survey datasets have geographic specifiers,
            # this specific implementation is to call out any extraneous handling for
            # multi-year handling (TODO).
            raise UnsupportedSpecException(
                f"The '{dataset}' API does not have supported geographic specifier handling "
                f"for Federal Informating Process Standard (FIPS) codes during the calendar "
                f"year {year}."
            )

    @classmethod
    def _url_fmt(cls, dataset: str, year: int) -> str:
        _dataset_meta_check(dataset, year)
        geography_url = 'https://api.census.gov/data/{}/{}/geography.json'.format(year, dataset)
        
        return geography_url


class ApiKeyConfig:
    """
    Formatter for user-defined Census Bureau API keys.
    """

    def __init__(self) -> None:
        self._FILE_PATH = Path.cwd() / 'api_key.txt'
        self._OS_ENV_LOCATION  = 'CENSUS_BUREAU_API_KEY'

        self._API_KEY = None

    @property
    def API_KEY(self):
        """
        The API key. Note that this can be directly set, if
        you prefer.
        """
        return self._API_KEY
    
    @API_KEY.setter
    def API_KEY(self, new_key: t.Any):
        self._API_KEY = new_key

    @property
    def OS_ENV_LOCATION(self):
        """
        The operation system (OS) environment location to the
        API key. Note that this is prioritized first.
        """
        return self._OS_ENV_LOCATION
    
    @OS_ENV_LOCATION.setter
    def OS_ENV_LOCATION(self, new_location: str):
        self._OS_ENV_LOCATION = new_location

    @property
    def FILE_PATH(self):
        """
        The textfile path containing the API key. Note that this
        is prioritized second.
        """
        return self._FILE_PATH
    
    @FILE_PATH.setter
    def FILE_PATH(self, new_file_path: t.Union[str, Path]):
        self._FILE_PATH = new_file_path

    def _get_api_key(self):
        self._set_api_key()

        if self.API_KEY:
            return f'&key={self.API_KEY}'
        else:
            logger.debug('Could not locate a Census Bureau API key.')
            return ''


    def _set_api_key(self):
        if not self.API_KEY:
            # First, check the operating system environment.
            key = os.environ.get(self.OS_ENV_LOCATION, None)

            # Next, check the file.
            if not key:
                try:
                    with open(self.FILE_PATH, 'r') as f:
                        key = f.readlines()[0]
                except:
                    key = None
            
            self._API_KEY = key

def _dataset_meta_check(dataset: str, year: int) -> None:
    if 'acs1' in dataset and year == 2020:
        raise APIException(
            f"The Census Bureau did not release 2020 estimates for the '{dataset}' dataset due "
            "to the impact of the COVID-19 pandemic on data collection efforts for 1-year estimate "
            "data. Nonetheless, experimental data for the American Community Survey's 1-year data "
            "estimates can be viewed at https://www.census.gov/programs-surveys/acs/data/experimental-data/1-year.html."
        )
    try:
        years = API_METADATA[dataset][0]
        
        if year not in years:
            raise APIException(
                f"Calendar year {year} was not supported for the '{dataset}' dataset. Supported "
                f"calendar years for this dataset include one of the following: {years}"
            )
        
    except KeyError:
        raise KeyError(
            f"'{dataset}' was not a recognizable/supported dataset. Supported datasets include "
            f"one of: {list(API_METADATA)}"
        ) from None



_InferSpec = namedtuple(
    '_InferSpec',
    ['spec', 'len', 'kwargs', 'supports_wildcard'],
    defaults = ([], None, {}, {})
)
"""
Custom tuple for fully-specified geographic paths specifying:
1. The list of geographic specifiers actually imputed into API queries.
2. The length of the fully-specified path, which is advantageous for strict/weak testing.
3. The keyword arguments that the user will actually supply, whose values correspond to
the specifiers that will be imputed into API queries.
4. Geographic specifiers that do and don't support wildcard operators (`'*'`).
"""