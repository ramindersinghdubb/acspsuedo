"""
:py:class:`~cache.DataCache` is an ABC for caching data.
"""
import shutil
import warnings
import typing as t
from pathlib import Path
from abc import ABC, abstractmethod



class DataCacheABC(ABC):
    """
    ABC for caching data.
    """
    @abstractmethod
    def __init__(
        self,
        is_cache: t.Optional[bool] = None,
        cache_path: t.Optional[t.Union[str, Path]] = None,
    ) -> None:
        """
        Parameters
        ----------
        is_cache
            If `True`, automatically cache the extracted shapefiles. If
            `False`, only fetch the shapefiles but do not extract. By
            default, this is 'None' (evaluated as `True`).

        cache_path
            The folder path to locally cache files. By default, this is
            `None` (evaluated as `True`) and thus creates a cache folder
            at `pathlib.Path.cwd() / 'cache'` if `auto_cache` is True.
        """
        raise NotImplementedError


    @property
    def is_cache(self) -> bool:
        """
        Indicates if data is written in a cache folder.
        """
        if self._is_cache is None:
            self._is_cache = True
        return self._is_cache
    
    @is_cache.setter
    def is_cache(self, cache_config: t.Optional[bool]) -> None:
        self._is_cache = cache_config

    @property
    def cache_path(self) -> t.Optional[Path]:
        """
        If data of any kind is written to a cache folder, return
        that cache path folder.
        """
        if self.is_cache:
            if self._cache_path is None:
                self._cache_path = Path.cwd() / 'cache'
            self._cache_path = Path(self._cache_path)
            self._cache_path.mkdir(parents = True, exist_ok = True)
            return self._cache_path
        else:
            return
        
    @cache_path.setter
    def cache_path(self, new_cache_path: t.Union[str, Path]) -> None:
        if self.is_cache:
            self._move_files(self.cache_path, new_cache_path)
        self._cache_path = new_cache_path
    
    def list_files(self) -> t.Optional[t.List[Path]]:
        """
        If caching, list all files in the cache folder.
        """
        if self.is_cache and self.cache_path:
            return [x for x in self.cache_path.iterdir() if x.is_file()]
        
    def _move_files(self, old_cache, new_cache: t.Union[str, Path]) -> None:
        """
        Internal for moving all files to new cache folder
        (if updated).
        """
        try:
            shutil.copytree(old_cache, new_cache, dirs_exist_ok=True)
            shutil.rmtree(old_cache)
        except:
            warnings.warn('This cache path already exists!', UserWarning)
            return