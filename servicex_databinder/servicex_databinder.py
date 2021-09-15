from typing import Union
import pathlib

from .configuration import _load_config
from .request import ServiceXRequest
from .frontend import ServiceXFrontend
from .output import _output_handler


class DataBinder:
    """ Manage and categorize numerous ServiceX data from a configuration file"""

    def __init__(self, config: Union[str, pathlib.Path]):
        self._config = _load_config(config)
        self._request = ServiceXRequest(self._config)

    def deliver(self, timer=False) -> None:

        # Configure ServiceX Frontend to connect ServiceX backend
        sx = ServiceXFrontend(self._config, self._request.get_requests())

        # print(f"Current cache: {sx.get_current_cache()}")
        current_cache = sx.get_current_cache()

        # Get a list of parquet files for each ServiceX request
        output_parquet_list = sx.get_servicex_data()

        # Handles ServiceX delivered output
        return _output_handler(self._config, self._request.get_requests(), output_parquet_list, current_cache)