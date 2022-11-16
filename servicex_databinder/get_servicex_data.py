from pathlib import Path
from typing import Any, Dict, List

from servicex import ServiceXDataset, utils, servicex_config
from aiohttp import ClientSession
import asyncio
from shutil import copy
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
        self._backend = self._config.get('General')['ServiceXBackendName'].lower()
        self._outputformat = self._config.get('General')['OutputFormat'].lower()

        self.output_handler = OutputHandler(config)
        self.output_path = self.output_handler.output_path
        self.out_paths_dict = self.output_handler.out_paths_dict
        self.update_out_paths_dict = self.output_handler.update_output_paths_dict
        self.add_local_output_paths_dict = self.output_handler.add_local_output_paths_dict
        self.parquet_to_root = self.output_handler.parquet_to_root
        
        self.ignoreCache = False
        if 'IgnoreServiceXCache' in self._config['General'].keys():
            self.ignoreCache = self._config['General']['IgnoreServiceXCache']
        self.failed_request = []        
        self.endpoint, _ = servicex_config.ServiceXConfigAdaptor(). \
                        get_servicex_adaptor_config \
                        (self._config['General']['ServiceXBackendName'])


    async def deliver_and_copy(self, req): 
        if 'uproot' in self._backend:
            target_path = Path(self.output_path, req['Sample'], req['tree'])
            title = f"{req['Sample']} - {req['tree']}"
        elif 'xaod' in self._backend:
            target_path = Path(self.output_path, req['Sample'])
            title = f"{req['Sample']}"
    
        if self._progresbar:
            callback_factory = None
        else:
            callback_factory = utils._run_default_wrapper

        try:
            async with ClientSession(timeout=3600) as session:
                sx_ds = ServiceXDataset(dataset=req['dataset'], 
                                        backend_name=self._config['General']['ServiceXBackendName'],
                                        status_callback_factory = callback_factory,
                                        session_generator=session,
                                        ignore_cache=self.ignoreCache
                        )
                query = req['query']
                files = await sx_ds.get_data_parquet_async(query, title=title)
        except Exception as e:
            self.failed_request.append({"request":req, "error":repr(e)})
            if 'uproot' in self._backend:
                return f"  Fail to deliver {req['Sample']} | {req['tree']} | {str(req['dataset'])[:100]}"
            elif 'xaod' in self._backend:
                return f"  Fail to deliver {req['Sample']} | {str(req['dataset'])[:100]}"
        
        # Update Outfile paths dictionary - add files based on the returned file list from ServiceX
        self.update_out_paths_dict(req, files, self._outputformat)

        # Copy
        if 'uproot' in self._backend:
            if not target_path.exists(): # easy - target directory doesn't exist
                target_path.mkdir(parents=True, exist_ok=True)            
                for file in files:
                    if self._outputformat == "parquet":
                        outfile = Path(target_path, Path(file).name)
                        copy(file, outfile)
                    elif self._outputformat == "root":
                        outfile = Path(target_path, Path(file).name).with_suffix('.root')
                        self.parquet_to_root(req['tree'], file, outfile)
                return f"  {req['Sample']} | {req['tree']} | {str(req['dataset'])[:100]}  is delivered"
            else: # hmm - target directory already there
                servicex_files = {Path(file).stem for file in files}
                local_files = {Path(file).stem for file in list(target_path.glob("*"))}
                if len(local_files):
                    if Path(list(target_path.glob("*"))[0]).suffix.strip(".") != self._outputformat:
                        for file in files:
                            if self._outputformat == "parquet":
                                outfile = Path(target_path, Path(file).name)
                                copy(file, outfile)
                            elif self._outputformat == "root":
                                outfile = Path(target_path, Path(file).name).with_suffix('.root')
                                self.parquet_to_root(req['tree'], file, outfile)
                        return f"  {req['Sample']} | {req['tree']} | {str(req['dataset'])[:100]} is delivered"

                if servicex_files == local_files: # one RucioDID for this sample and files are already there
                    return f"  {req['Sample']} | {req['tree']} | {str(req['dataset'])[:100]} is already delivered"
                else:
                    # copy files in servicex but not in local
                    files_not_in_local = servicex_files.difference(local_files)
                    if files_not_in_local:
                        for file in files_not_in_local:
                            if self._outputformat == "parquet":
                                copy(Path(Path(files[0]).parent, file+".parquet"), Path(target_path, file+".parquet"))
                            elif self._outputformat == "root":
                                self.parquet_to_root(req['tree'], Path(Path(files[0]).parent, file+".parquet"), Path(target_path, file+".root"))
                        return f"  {req['Sample']} | {req['tree']} | {str(req['dataset'])[:100]} is delivered"
                    else:
                        return f"  {req['Sample']} | {req['tree']} | {str(req['dataset'])[:100]} is already delivered"

        elif 'xaod' in self._backend:    
            if not target_path.exists(): # easy - target directory doesn't exist
                target_path.mkdir(parents=True, exist_ok=True)            
                for file in files:
                    outfile = Path(target_path, Path(file).name)
                    copy(file, outfile)
                return f"  {req['Sample']} | {str(req['dataset'])[:100]}  is delivered"
            else: # hmm - target directory already there
                servicex_files = {Path(file).name for file in files}
                local_files = {Path(file).name for file in list(target_path.glob("*"))}
                if servicex_files == local_files: # one RucioDID for this sample and files are already there
                    return f"  {req['Sample']} | {str(req['dataset'])[:100]} is already delivered"
                else:
                    # copy files in servicex but not in local
                    files_not_in_local = servicex_files.difference(local_files)
                    if files_not_in_local:
                        for file in files_not_in_local:
                            copy(Path(Path(files[0]).parent, file), Path(target_path, file))
                        return f"  {req['Sample']} | {str(req['dataset'])[:100]} is delivered"
                    else:
                        return f"  {req['Sample']} | {str(req['dataset'])[:100]} is already delivered"


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
