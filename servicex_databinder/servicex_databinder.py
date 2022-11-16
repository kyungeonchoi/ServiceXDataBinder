from typing import Union, Dict, Any
from pathlib import Path
import time
import asyncio
from threading import Thread

from .configuration import _load_config
from .request import ServiceXRequest
from .get_servicex_data import DataBinderDataset
from .output_handler import OutputHandler

import logging
log = logging.getLogger(__name__)

class DataBinder:
    """ Manage and categorize numerous ServiceX data from a configuration file"""

    def __init__(self, config: Union[str, Path, Dict[str, Any]]):
        self._config = _load_config(config)
        self._requests = ServiceXRequest(self._config).get_requests()
        self._sx_db = DataBinderDataset(self._config, self._requests)

        log.info(f"  {len(self._config.get('Sample'))} Samples and {len(self._requests)} ServiceX requests")

    def deliver(self, overall_progress_only: bool = False) -> Dict:

        out_paths_dict = asyncio.run(self._sx_db.get_data(overall_progress_only))

        # OutputHandler(self._config).clean_up_files_not_in_requests(out_paths_dict)

        x = Thread(target=OutputHandler(self._config).clean_up_files_not_in_requests, args=(out_paths_dict,))
        x.start()

        if len(self._sx_db.failed_request):
            log.warning(f"{len(self._sx_db.failed_request)} failed delivery request(s)")
            log.warning(f"get_failed_requests() for detail of failed requests")

        return out_paths_dict

    def get_failed_requests(self):
        return self._sx_db.failed_request