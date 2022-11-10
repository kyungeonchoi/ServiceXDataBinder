from typing import Union, Dict
from pathlib import Path
import logging
import time

# from servicex import ServiceXDataset
# from aiohttp import ClientSession
# from aiofiles import os
import asyncio

# import nest_asyncio
# nest_asyncio.apply()

from .configuration import _load_config
from .request import ServiceXRequest
from .get_servicex_data import DataBinderDataset
# from .frontend import ServiceXFrontend
# from .output import _output_handler

log = logging.getLogger(__name__)

class DataBinder:
    """ Manage and categorize numerous ServiceX data from a configuration file"""

    def __init__(self, config: Union[str, Path]):
        self._config = _load_config(config)
        self._requests = ServiceXRequest(self._config).get_requests()
        self._sx_db = DataBinderDataset(self._config, self._requests)
        # self._sx = ServiceXFrontend(self._config, self._requests)
        # self._cache_before_requests = self._sx.get_current_cache()



    def deliver(self, timer=False) -> Dict:

        return asyncio.run(self._sx_db.get_data())
        
        # """Get a list of parquet files for each ServiceX request"""
        # t_0 = time.perf_counter()
        # try:
        #     output_parquet_list = self._sx.get_servicex_data()
        # except:
        #     log.exception("Exception occured while gettting data via ServiceX")
        #     raise
        # t_1 = time.perf_counter()

        # """Handles ServiceX delivered output"""
        # out = _output_handler(self._config, self._requests, output_parquet_list, self._cache_before_requests)
        # t_2 = time.perf_counter()

        # if timer:
        #     print(f"---------------- Timer ----------------")
        #     print(f"ServiceX data delivery: {t_1-t_0:0.1f} seconds")
        #     print(f"Post-processing       : {t_2-t_1:0.1f} seconds")

        # return out