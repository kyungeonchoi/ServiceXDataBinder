from pathlib import Path
from typing import Any, Dict, List

from servicex import ServiceXDataset
from aiohttp import ClientSession
from aiofiles import os
import asyncio

from .output_handler import OutputHandler

import nest_asyncio
nest_asyncio.apply()

import logging
log = logging.getLogger(__name__)

class DataBinderDataset:

    def __init__(self, config: Dict[str, Any], servicex_requests: List):
        self._config = config
        self._servicex_requests = servicex_requests
        self._backend = self._config.get('General')['ServiceXBackendName'].lower()

        self.output_handler = OutputHandler(config)
        self.output_path = self.output_handler.get_outpath()
        self.out_paths_dict = self.output_handler.out_paths_dict
        
        self.ignoreCache = False
        if 'IgnoreServiceXCache' in self._config['General'].keys():
            self.ignoreCache = self._config['General']['IgnoreServiceXCache']


    async def deliver_and_copy(self, req):
        # print(f"ServiceX running for {req['Sample']}")
        async with ClientSession() as session:
            sx_ds = ServiceXDataset(dataset=req['dataset'], 
                                    backend_name=self._config['General']['ServiceXBackendName'],
                                    session_generator=session,
                                    ignore_cache=self.ignoreCache
                    )
            query = req['query']
            title = f"{req['Sample']} - {req['tree']}"
            files = await sx_ds.get_data_parquet_async(query, title=title)

        # print(f"Copying files for {req['Sample']}")
        target_path = Path(self.output_path, req['Sample'], req['tree'])

        # Add files based on the returned file list from ServiceX 
        self.out_paths_dict[req['Sample']][req['tree']].extend([str(Path(target_path, Path(file).name)) for file in files])

        if not target_path.exists(): # easy - target directory doesn't exist
            target_path.mkdir(parents=True, exist_ok=True)
            for file in files:
                outfile = Path(target_path, Path(file).name)
                await os.link(file, outfile)
        else: # hmm - target directory already there
            servicex_files = {Path(file).name for file in files}
            local_files = {Path(file).name for file in list(target_path.glob("*"))}
            
            if servicex_files == local_files: # one RucioDID for this sample and files are already there
                pass
            else:                
                # copy files in servicex but not in local
                files_not_in_local = servicex_files.difference(local_files)
                for file in files_not_in_local:
                    await os.link(Path(Path(files[0]).parent, file), Path(target_path, file))

                # # delete files in local but not in servicex - cannot do this because more than 1 Rucio DID can exist for given sample
                # files_not_in_servicex = local_files.difference(servicex_files)
                # for file in files_not_in_servicex:
                #     await os.unlink(Path(target_path, file))

        log.info(f"{req['Sample']} - {req['tree']} is delivered")


        
        # return 

    async def get_data(self):
        log.info(f"Deliver via ServiceX endpoint: {self._config['General']['ServiceXBackendName']}")
        log.info("Samples in the config file")

        tasks = []

        for req in self._servicex_requests:
            log.info(f"   {req['Sample']} - {req['tree']}")
            tasks.append(self.deliver_and_copy(req))
        
        await asyncio.gather(*tasks)
        
        self.output_handler.write_output_paths_dict(self.out_paths_dict)

        return self.out_paths_dict
