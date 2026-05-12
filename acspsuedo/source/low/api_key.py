"""
Interface for handling the (as of May 2026) now
mandatory API key in order to peruse the Census
Bureau API.
"""
from __future__ import annotations
import os
import typing as t
from pathlib import Path
from logging import getLogger



from acspsuedo.source.low.exceptions import ApiKeyException


logger = getLogger(__name__)



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

        if not self._API_KEY:
            raise ApiKeyException(
                "As of May 2026, the Census Bureau requires users to supply their "
                "API keys in order to peruse the Bureau's API. You can sign up "
                "for a free API key at https://api.census.gov/data/key_signup.html."
            )
        return f'key={self.API_KEY}'


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


api_key_config: ApiKeyConfig = ApiKeyConfig()