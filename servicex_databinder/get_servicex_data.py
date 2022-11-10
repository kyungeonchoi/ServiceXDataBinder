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
                    
        if 'OutputDirectory' in config['General'].keys():
            self.output_path = Path(config['General']['OutputDirectory']).absolute()
            self.output_path.mkdir(parents=True, exist_ok=True)
        else:
            self.output_path = Path('ServiceXData').absolute()
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
            title = f"{req['Sample']} - {req['tree']}"
            files = await sx_ds.get_data_parquet_async(query, title=title)

        print(f"Copying files for {req['Sample']}")
        target_path = Path(self.output_path, req['Sample'], req['tree'])

        if not target_path.exists():
            target_path.mkdir(parents=True, exist_ok=True)
            for file in files:
                outfile = Path(target_path, Path(file).name)
                await os.link(file, outfile)
        else:
            servicex_files = {Path(file).name for file in files}
            local_files = {Path(file).name for file in list(target_path.glob("*"))}
            
            if servicex_files == local_files:
                pass
            else:
                # delete files in local but not in servicex
                files_not_in_servicex = local_files.difference(servicex_files)
                for file in files_not_in_servicex:
                    await os.unlink(Path(target_path, file))

                # copy files in servicex but not in local
                files_not_in_local = servicex_files.difference(local_files)
                for file in files_not_in_local:
                    await os.link(Path(Path(files[0]).parent, file), Path(target_path, file))

        # return 

    async def get_data(self):
        tasks = []

        for req in self._servicex_requests:
            tasks.append(self.deliver_and_copy(req))

        return await asyncio.gather(*tasks)