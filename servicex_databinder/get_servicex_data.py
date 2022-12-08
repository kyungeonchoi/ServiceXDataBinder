from pathlib import Path
from typing import Any, Dict, List

from servicex import ServiceXDataset, utils, servicex_config
from aiohttp import ClientSession
import asyncio

from tqdm.asyncio import tqdm

from .output_handler import OutputHandler

import nest_asyncio
nest_asyncio.apply()

import logging
log = logging.getLogger(__name__)

class DataBinderDataset:

    def __init__(self, config: Dict[str, Any], servicex_requests: List):
        self._config = config
        self._servicex_requests = servicex_requests
        self._backend_name = self._config.get('General')['ServiceXBackendName']
        self._backend_type = servicex_config.ServiceXConfigAdaptor().get_backend_info(self._backend_name, "type")
        self._outputformat = self._config.get('General')['OutputFormat'].lower()
        self.transformerImage = None
        if 'TransformerImage' in self._config['General'].keys():
            self.transformerImage = self._config['General']['TransformerImage']

        self.output_handler = OutputHandler(config)
        self.out_paths_dict = self.output_handler.out_paths_dict
        self.update_out_paths_dict = self.output_handler.update_output_paths_dict
        self.add_local_output_paths_dict = self.output_handler.add_local_output_paths_dict
        
        self.ignoreCache = False
        if 'IgnoreServiceXCache' in self._config['General'].keys():
            self.ignoreCache = self._config['General']['IgnoreServiceXCache']
        self.failed_request = []        
        self.endpoint, _ = servicex_config.ServiceXConfigAdaptor(). \
                        get_servicex_adaptor_config(self._backend_name)


    async def deliver_and_copy(self, req):         

        if (self._backend_type, self._outputformat) == ('uproot', 'root'):
            title = f"{req['Sample']} - {req['tree']}"
            message_fail = f"  Fail to deliver {req['Sample']} | {req['tree']} | {str(req['dataset'])[:100]}"
        elif (self._backend_type, self._outputformat) == ('uproot', 'parquet'):
            title = f"{req['Sample']} - {req['tree']}"
            message_fail = f"  Fail to deliver {req['Sample']} | {req['tree']} | {str(req['dataset'])[:100]}"
        elif (self._backend_type, self._outputformat) == ('xaod', 'root'):
            title = f"{req['Sample']}"
            message_fail = f"  Fail to deliver {req['Sample']} | {str(req['dataset'])[:100]}"
    
        if self._progresbar:
            callback_factory = None
        else:
            callback_factory = utils._run_default_wrapper

        try:
            async with ClientSession(timeout=3600) as session:
                sx_ds = ServiceXDataset(dataset=req['dataset'], 
                                        backend_name=self._backend_name,
                                        image=self.transformerImage,
                                        status_callback_factory = callback_factory,
                                        session_generator=session,
                                        ignore_cache=self.ignoreCache
                        )
                query = req['query']
                files = await sx_ds.get_data_parquet_async(query, title=title)
        except Exception as e:
            self.failed_request.append({"request":req, "error":repr(e)})
            return message_fail
        
        # Update Outfile paths dictionary - add files based on the returned file list from ServiceX
        self.update_out_paths_dict(req, files, self._outputformat)

        # Copy
        try:
            return await self.output_handler.copy_files(req, files)
        except Exception as e:
            self.failed_request.append({"request":req, "error":repr(e)})
            return message_fail

       
    async def get_data(self, overall_progress_only):
        log.info(f"Deliver via ServiceX endpoint: {self.endpoint}")

        self._progresbar = overall_progress_only

        tasks = []

        for req in self._servicex_requests:
            tasks.append(self.deliver_and_copy(req))
        
        # await asyncio.gather(*tasks)

        if overall_progress_only:
            pbar = tqdm(total=len(tasks),
                            unit="request",
                            dynamic_ncols=True,
                            colour='#ffa500',
                            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]",
                            )
        for f in asyncio.as_completed(tasks):
            value = await f
            if overall_progress_only:
                pbar.set_description(value)
                pbar.update()
            else:
                log.info(value)

        if overall_progress_only: pbar.close()

        self.add_local_output_paths_dict()

        self.output_handler.write_output_paths_dict(self.out_paths_dict)

        return self.out_paths_dict
