from pathlib import Path
import logging
from typing import Any, Dict, List

from servicex import ServiceXDataset
from aiohttp import ClientSession
from aiofiles import os
import asyncio

import nest_asyncio
nest_asyncio.apply()

log = logging.getLogger(__name__)

class DataBinderDataset:

    def __init__(self, config: Dict[str, Any], servicex_requests: List):
        self._config = config
        self._servicex_requests = servicex_requests
        self._backend = self._config.get('General')['ServiceXBackendName'].lower()
        
        self.ignoreCache = False
        if 'IgnoreServiceXCache' in self._config['General'].keys():
            self.ignoreCache = self._config['General']['IgnoreServiceXCache']
            
        self.output_path = Path('ServiceXData').absolute()
        self.output_path.mkdir(parents=True, exist_ok=True)
        if 'OutputDirectory' in config['General'].keys():
            self.output_path = Path(config['General']['OutputDirectory']).absolute()
            self.output_path.mkdir(parents=True, exist_ok=True)


    async def deliver_and_copy(self, req):
        print(f"ServiceX running for {req['Sample']}")
        async with ClientSession() as session:
            sx_ds = ServiceXDataset(dataset=req['dataset'], 
                                    backend_name=self._config['General']['ServiceXBackendName'],
                                    session_generator=session,
                                    ignore_cache=self.ignoreCache
                    )
            query = req['query']
            files = await sx_ds.get_data_parquet_async(query)

        # shutil.rmtree to remove target directory

        print(f"Copying files for {req['Sample']}")        
        Path(self.output_path, req['Sample'], req['tree']).mkdir(parents=True, exist_ok=True)
        for file in files:
            outfile = Path(self.output_path, req['Sample'], Path(file).name)
            await os.link(file, outfile)


    async def get_data(self):
        tasks = []

        for req in self._servicex_requests:

            # Check local cache and only append new request

            tasks.append(self.deliver_and_copy(req))

        return await asyncio.gather(*tasks)