from typing import Union, Dict
import pathlib
import logging

from .configuration import _load_config
from .request import ServiceXRequest
from .frontend import ServiceXFrontend
from .output import _output_handler

log = logging.getLogger(__name__)

class DataBinder:
    """ Manage and categorize numerous ServiceX data from a configuration file"""

    def __init__(self, config: Union[str, pathlib.Path]):
        self._config = _load_config(config)
        self._requests = ServiceXRequest(self._config).get_requests()
        self._sx = ServiceXFrontend(self._config, self._requests)
        self._cache_before_requests = self._sx.get_current_cache()
        

    def deliver(self, timer=False) -> Dict:
        
        """Get a list of parquet files for each ServiceX request"""
        try:
            output_parquet_list = self._sx.get_servicex_data()
        except:
            log.exception("Exception occured while gettting data via ServiceX")
            raise

        """Handles ServiceX delivered output"""
        return _output_handler(self._config, self._requests, output_parquet_list, self._cache_before_requests)