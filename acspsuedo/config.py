"""
Internal configuration settings.

This will be how any changes to caching, API key data,
and/or TIGER shapefile extraction are handled.
"""
import typing as t
from pathlib import Path
import pandas as pd

from acspsuedo.geographies import ShapeFileHandler



class ConfigSettings(ShapeFileHandler):
    """Class for configuration settings."""
    def __init__(
        self,
        api_key: t.Optional[str] = None,
        year: int = 2020,
        is_cache: t.Optional[bool] = None,
        cache_path: t.Optional[t.Union[str, Path]] = None
    ):
        """
        Initialization for :py:class:`~config.ConfigSettings`.

        Parameters
        ----------
        api_key
            An API key. This is recommended for users querying multiple
            (50+) datasets in a session.

        year
            Shapefile year.

        is_cache
            If `True`, automatically cache the extracted shapefiles. If
            `False`, only fetch the shapefiles but do not extract. By
            default, this is 'None' (evaluated as `True`).

        cache_path
            The folder path to locally cache files. By default, this is
            `None` (evaluated as `True`) and thus creates a cache folder
            at `pathlib.Path.cwd() / 'cache'` if `auto_cache` is True.
        """
        self._api_key = api_key
        super().__init__(year, is_cache, cache_path)


    @property
    def api_key(self) -> str:
        """The API key."""
        if self._api_key is None:
            return ''
        return self._api_key
    
    @api_key.setter
    def api_key(self, new_key: t.Optional[str]) -> None:
        self._api_key = new_key

    
    def write_api_file(self):
        """If caching, write the API key to the cache folder."""
        if self.is_cache and self.cache_path:
            with open(self.cache_path / 'API_key.txt', 'w') as f:
                f.write(self.api_key)

    def get_api_key(self) -> str:
        """
        If caching, retrieve the API key from the cache. Else,
        retrieve from the instance.
        """
        if self.is_cache and self.cache_path:
            file = self.cache_path / 'API_key.txt'
            self.write_api_file()
            file = open(self.cache_path / 'API_key.txt', 'r')
            api_key = file.readline()
            return self._api_key_fmt(api_key)
        else:
            return self._api_key_fmt(self.api_key)
        
    def _api_key_fmt(self, api_key):
        """
        Internal for returning a(n) formatter for the API key (if found).
        """
        if api_key:
            return '&key={}'.format(api_key)
        else:
            return ''
        
    @classmethod
    def _acs_df_cleaner(cls, label: str, content: t.List[t.List[t.Any]]) -> pd.DataFrame:
        """
        Internal for cleaning and formatting fetched American
        Community Survey data.
        """
        # JSON content returns an array of arrays
        df = pd.DataFrame(
            columns = content[0],
            data    = content[1:],
            dtype   = object
        )

        # Drop annotation variables
        anno_cols = [i for i in df.columns if i.startswith(label) and i.endswith('A')]
        df.drop(anno_cols, axis = 1, inplace = True)

        # Format 'GEO_ID' to correspond w/ TIGER shapefiles
        df['GEOID'] = [i.split('US')[-1] for i in df['GEO_ID']]
        df.drop(['GEO_ID'], axis = 1, inplace=True)
        df.sort_values(by=['GEOID'], inplace=True)

        # Identification information to the left-most of the dataframe
        label_cols = [i for i in df.columns if i.startswith(label)]
        id_cols    = [i for i in df.columns if i not in label_cols]

        df = df[id_cols + label_cols]

        # Reset index
        df.reset_index(drop = True, inplace = True)

        return df