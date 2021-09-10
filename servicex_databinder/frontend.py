import asyncio
from servicex import ServiceXDataset
from aiohttp import ClientSession
import nest_asyncio
from typing import Any, Dict, List, Optional, Union

# import sys
# sys.path.insert(1,"/Users/kchoi/ServiceX/ServiceX_frontend")
# from servicex import ServiceXDataset

class ServiceXFrontend:

    def __init__(self, config: Dict[str, Any], servicex_requests):
        """
        self._list_sx_dataset_query_pair   List of ServiceX dataset and query pair
        """
        self._config = config
        self._servicex_requests = servicex_requests

    def get_servicex_data(self, test_run=False):
        """
        Get data from ServiceX
        """
        print("1/4 Retrieving data from ServiceX Uproot backend..")

        nest_asyncio.apply()
 
        async def bound_get_data(sem, sx_ds, query):
            async with sem:
                return await sx_ds.get_data_parquet_async(query)
        # async def bound_get_data(sem, sx_ds, query, sample):
        #     async with sem:
        #         return await sx_ds.get_data_parquet_async(selection_query=query, title=sample)

        async def _get_my_data():
            sem = asyncio.Semaphore(50) # Limit maximum concurrent ServiceX requests
            tasks = []
            uproot_transformer_image = "sslhep/servicex_func_adl_uproot_transformer:develop"
            async with ClientSession() as session:
                for request in self._servicex_requests:
                    sx_ds = ServiceXDataset(dataset=request['gridDID'], \
                        backend_name=self._config['General']['ServiceXBackendName'], \
                        image=uproot_transformer_image, \
                        session_generator=session, \
                        ignore_cache=self._config['General']['IgnoreServiceXCache'])
                    query = request['query']

                    # task = asyncio.ensure_future(bound_get_data(sem, sx_ds, query, request['Sample']))
                    task = asyncio.ensure_future(bound_get_data(sem, sx_ds, query))
                    tasks.append(task)
                return await asyncio.gather(*tasks)

        newloop = asyncio.get_event_loop()
        data = newloop.run_until_complete(_get_my_data())
        print("2/4 Complete ServiceX data delivery..")
        return data
