from typing import Any, Dict, List
import logging

from aiohttp import ClientSession
import asyncio
from tqdm.asyncio import tqdm

from servicex import ServiceXDataset, utils, servicex_config
from .output_handler import OutputHandler

import nest_asyncio
nest_asyncio.apply()

log = logging.getLogger(__name__)


class DataBinderDataset:

    def __init__(self, config: Dict[str, Any], servicex_requests: List):
        self._config = config
        self._servicex_requests = servicex_requests
        self._outputformat \
            = self._config.get('General')['OutputFormat'].lower()
        self.transformerImage = None
        self.output_handler = OutputHandler(config)
        self.ignoreCache = False
        if 'IgnoreServiceXCache' in self._config['General'].keys():
            self.ignoreCache = self._config['General']['IgnoreServiceXCache']
        self.failed_request = []
        self.endpoint = servicex_config.ServiceXConfigAdaptor()\
            .get_backend_info(
                self._config['General']['ServiceXName'],
                "endpoint"
                )

    async def deliver_and_copy(self, req, delivery_setting):
        if req['codegen'] == "uproot":
            title = f"{req['Sample']} - {req['tree']}"
        elif req['codegen'] == "atlasr21" or req['codegen'] == "python":
            title = f"{req['Sample']}"

        if self._progresbar:
            callback_factory = None
        else:
            callback_factory = utils._run_default_wrapper

        try:
            async with ClientSession(timeout=3600) as session:
                sx_ds = ServiceXDataset(
                    dataset=req['dataset'],
                    backend_name=self._config['General']['ServiceXName'],
                    backend_type=req['type'],
                    codegen=req['codegen'],
                    # image=self.transformerImage,
                    status_callback_factory=callback_factory,
                    session_generator=session,
                    ignore_cache=self.ignoreCache
                    )
                query = req['query']
                if delivery_setting == 1 or delivery_setting == 3:
                    files = await sx_ds.get_data_parquet_async(
                        query,
                        title=title
                        )
                elif delivery_setting == 2 or delivery_setting == 4:
                    files = await sx_ds.get_data_rootfiles_async(
                        query,
                        title=title
                        )
                elif delivery_setting == 5:
                    files = await sx_ds.get_data_parquet_uri_async(
                        query,
                        title=title
                        )
                elif delivery_setting == 6:
                    files = await sx_ds.get_data_rootfiles_uri_async(
                        query,
                        title=title
                        )

            # Update Outfile paths dictionary
            self.output_handler.update_output_paths_dict(
                req, files, delivery_setting
                )

            self.output_handler.copy_to_target(delivery_setting, req, files)
        except Exception as e:
            self.failed_request.append({"request": req, "error": repr(e)})
            if req['codegen'] == "uproot":
                return ("  Fail to deliver "
                        f"{req['Sample']} | "
                        f"{req['tree']} | "
                        f"{str(req['dataset'])[:100]}")
            elif req['codegen'] == "atlasr21":
                return ("  Fail to deliver "
                        f"{req['Sample']} | "
                        f"{str(req['dataset'])[:100]}")

    async def get_data(self, overall_progress_only):
        log.info(f"Deliver via ServiceX endpoint: {self.endpoint}")

        if self._outputformat == "parquet" and \
                self._config['General']['Delivery'] == "localpath":
            delivery_setting = 1
        elif self._outputformat == "root" and \
                self._config['General']['Delivery'] == "localpath":
            delivery_setting = 2
        elif self._outputformat == "parquet" and \
                self._config['General']['Delivery'] == "localcache":
            delivery_setting = 3
        elif self._outputformat == "root" and \
                self._config['General']['Delivery'] == "localcache":
            delivery_setting = 4
        elif self._outputformat == "parquet" and \
                self._config['General']['Delivery'] == "objectstore":
            delivery_setting = 5
        elif self._outputformat == "root" and \
                self._config['General']['Delivery'] == "objectstore":
            delivery_setting = 6

        self._progresbar = overall_progress_only

        tasks = []

        for req in self._servicex_requests:
            tasks.append(self.deliver_and_copy(req, delivery_setting))

        if overall_progress_only:
            barformat = "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]"
            pbar = tqdm(total=len(tasks),
                        unit="request",
                        dynamic_ncols=True,
                        colour='#ffa500',
                        bar_format=barformat,
                        )
        for f in asyncio.as_completed(tasks):
            value = await f
            if overall_progress_only:
                pbar.set_description(value)
                pbar.update()
            else:
                pass

        if overall_progress_only:
            pbar.close()

        self.output_handler.add_local_output_paths_dict()

        if delivery_setting == 1 or delivery_setting == 2:
            log.info(f"Delivered at {self.output_handler.output_path}")

        self.output_handler.write_output_paths_dict(
            self.output_handler.out_paths_dict
            )

        return self.output_handler.out_paths_dict
