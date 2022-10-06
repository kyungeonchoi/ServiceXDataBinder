import asyncio
from servicex import ServiceXDataset
from aiohttp import ClientSession
import nest_asyncio
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import logging

from .output import get_tree_name

log = logging.getLogger(__name__)

class ServiceXFrontend:

    def __init__(self, config: Dict[str, Any], servicex_requests):
        """
        self._list_sx_dataset_query_pair   List of ServiceX dataset and query pair
        """
        self._config = config
        self._servicex_requests = servicex_requests
        self._backend = self._config.get('General')['ServiceXBackendName'].lower()

    def get_current_cache(self) -> List:        
        cache_path = ServiceXDataset("",backend_name=self._config['General']['ServiceXBackendName'])._cache._path
        log.debug(f"local cache path: {cache_path}")
        query_cache_status = Path.joinpath(cache_path, "query_cache_status")
        return list(query_cache_status.glob('*'))

    def get_servicex_data(self, test_run=False):
        """
        Get data from ServiceX
        """
        log.info(f"retrieving data via {self._config['General']['ServiceXBackendName']} ServiceX..")

        if 'IgnoreServiceXCache' in self._config['General'].keys():
            ignoreCache = self._config['General']['IgnoreServiceXCache']
        else:
            ignoreCache = False

        if 'uproot' in self._backend:
            transformer_image = "sslhep/servicex_func_adl_uproot_transformer:develop"
        elif 'xaod' in self._backend:
            transformer_image = "sslhep/servicex_func_adl_xaod_transformer:develop"
        
        nest_asyncio.apply()
 
        async def bound_get_data(sem, sx_ds, query, title):
            async with sem:
                return await sx_ds.get_data_parquet_async(query, title)

        async def _get_my_data():
            sem = asyncio.Semaphore(50) # Limit maximum concurrent ServiceX requests
            tasks = []
            
            async with ClientSession() as session:
                for request in self._servicex_requests:
                    sx_ds = ServiceXDataset(dataset=request['dataset'], \
                                            backend_name=self._config['General']['ServiceXBackendName'], \
                                            # image=transformer_image, \
                                            session_generator=session, \
                                            ignore_cache=ignoreCache)
                    query = request['query']
                    
                    if 'uproot' in self._backend:
                        title = request['Sample'] + ' - ' + get_tree_name(query)
                    else:
                        title = request['Sample']
                        
                    task = asyncio.ensure_future(bound_get_data(sem, sx_ds, query, title))
                    tasks.append(task)
                return await asyncio.gather(*tasks)

        newloop = asyncio.get_event_loop()
        data = newloop.run_until_complete(_get_my_data())
        log.info(f"complete ServiceX data delivery..")
        return data
