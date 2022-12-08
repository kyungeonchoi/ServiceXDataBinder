import yaml
from pathlib import Path
from typing import Any, Dict
import time
from shutil import rmtree
from aioshutil import copy

import pyarrow.parquet as pq
import awkward as ak
import uproot

import logging
log = logging.getLogger(__name__)

class OutputHandler:

    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = config
        if "uproot" in config['General']['ServiceXBackendName'].lower():
            self._backend = "uproot"
        elif "xaod" in config['General']['ServiceXBackendName'].lower():
            self._backend = "xaod"
        self._outputformat = self._config.get('General')['OutputFormat'].lower()

        """Prepare output path dictionary"""        
        out_paths = {}    
        samples = []
        [samples.append(sample['Name']) for sample in config['Sample'] if sample['Name'] not in samples]
        for sample in samples:
            out_paths[sample] = {}
        if self._outputformat == "parquet":
            for sample in config['Sample']:
                for tree in sample['Tree'].split(','):
                    out_paths[sample['Name']][tree.strip()] = []
        self.out_paths_dict = out_paths

        """Create output directory"""
        if 'OutputDirectory' in self._config['General'].keys():
            self.output_path = Path(self._config['General']['OutputDirectory']).absolute()
            self.output_path.mkdir(parents=True, exist_ok=True)
        else:
            self.output_path = Path('ServiceXData').absolute()
            self.output_path.mkdir(parents=True, exist_ok=True)


    ##################################
    ### parquet to ROOT conversion ###
    ##################################
    def parquet_to_root(self, tree_name, pq_file, root_file):
        """Write ROOT ntuple from parquet file"""
        if pq.read_metadata(pq_file).num_rows == 0:
            pass
        else:
            outfile = uproot.recreate(root_file)
            tree_dict = {}
            ak_arr = ak.from_parquet(pq_file)
            for field in ak_arr.fields:
                tree_dict[field] = ak_arr[field]
            outfile[tree_name] = tree_dict
            outfile.close()


    #################################
    ### Copy, update and clean up ###
    #################################
    
    async def copy_files(self, req, files):
        if (self._backend, self._outputformat) == ('uproot', 'root'):
            target_path = Path(self.output_path, req['Sample'])
            if not target_path.exists():
                target_path.mkdir(parents=True, exist_ok=True)
                for file in files:
                    outfile = Path(target_path, Path(file).name)
                    await copy(file, outfile)
                return f"  {req['Sample']} | {req['tree']} | {str(req['dataset'])[:100]} is delivered"
            else: # target directory exists
                servicex_files = {Path(file).name for file in files}
                local_files = {Path(file).name for file in list(target_path.glob("*"))}
                if servicex_files == local_files: # one RucioDID for this sample and files are already there
                    return f"  {req['Sample']} | {req['tree']} | {str(req['dataset'])[:100]} is already delivered"
                else:
                    # copy files in servicex but not in local
                    files_not_in_local = servicex_files.difference(local_files)
                    if files_not_in_local:
                        for file in files_not_in_local:
                            await copy(Path(Path(files[0]).parent, file), Path(target_path, file))
                        return f"  {req['Sample']} | {req['tree']} | {str(req['dataset'])[:100]} is delivered"
                    else:
                        return f"  {req['Sample']} | {req['tree']} | {str(req['dataset'])[:100]} is already delivered"


    def clean_up_files_not_in_requests(self, out_paths_dict):

        samples_in_requests = out_paths_dict.keys()
        samples_local = [sa.name for sa in self.output_path.iterdir() if sa.is_dir()]

        if self._backend == "uproot":
            for sample in samples_local:
                if not sample in samples_in_requests:
                    rmtree(Path(self.output_path, sample))
                else:
                    if self._outputformat == "parquet":
                        for tree in [tr.name for tr in Path(self.output_path, sample).iterdir() if tr.is_dir()]:
                            if tree in out_paths_dict[sample].keys():
                                files_local = set(Path(self.output_path, sample, tree).glob("*"))
                                files_request = set([Path(item) for item in out_paths_dict[sample][tree]])
                                for tbd in files_local.difference(files_request):
                                    Path.unlink(tbd)
                            else:
                                rmtree(Path(self.output_path, sample, tree))
                        # delete files if output format was root before
                        for fi in [fi.name for fi in Path(self.output_path, sample).iterdir() if fi.is_file()]:
                            Path.unlink(Path(self.output_path, sample, fi))
                    elif self._outputformat == "root":
                        for tree in [tr.name for tr in Path(self.output_path, sample).iterdir() if tr.is_dir()]:
                            rmtree(Path(self.output_path, sample, tree))                            
                        files_local = set(Path(self.output_path, sample).glob("*"))
                        files_request = set([Path(item) for item in out_paths_dict[sample]])
                        for tbd in files_local.difference(files_request):
                            Path.unlink(tbd)
                        
        elif self._backend == "xaod":
            for sample in samples_local:
                if not sample in samples_in_requests:
                    rmtree(Path(self.output_path, sample))
                else:
                    files_local = set(Path(self.output_path, sample).glob("*"))
                    files_request = set([Path(item) for item in out_paths_dict[sample]])
                    for tbd in files_local.difference(files_request):
                        Path.unlink(tbd)

        return

    
    ########################################
    ### Dictionary for output file paths ###
    ########################################
    def update_output_paths_dict(self, req, files, format:str = "parquet"):
        if self._outputformat == "parquet":
            # Outfile paths dictionary - add files based on the returned file list from ServiceX
            target_path = Path(self.output_path, req['Sample'], req['tree'])
            paths_in_output_dict = self.out_paths_dict[req['Sample']][req['tree']]
            new_files = [str(Path(target_path, Path(file).name)) for file in files]
            if paths_in_output_dict:
                output_dict = list(set(paths_in_output_dict + new_files))
            else:
                output_dict = new_files
            self.out_paths_dict[req['Sample']][req['tree']] = output_dict
        elif self._outputformat == "root":
            target_path = Path(self.output_path, req['Sample'])
            paths_in_output_dict = self.out_paths_dict[req['Sample']]
            new_files = [str(Path(target_path, Path(file).name)) for file in files]
            if paths_in_output_dict:
                output_dict = list(set(paths_in_output_dict + new_files))
            else:
                output_dict = new_files
            self.out_paths_dict[req['Sample']] = output_dict


    def add_local_output_paths_dict(self):
        local_samples = [sample for sample in self._config.get('Sample') if 'LocalPath' in sample.keys()]
        for sample in local_samples:
            if self._outputformat == "parquet":
                for tree, fpath in zip(sample['Tree'].split(','), sample['LocalPath'].split(',')):
                    tree = tree.strip()
                    fpath = fpath.strip()
                    self.out_paths_dict[sample['Name']][tree] = [str(Path(f)) for f in Path(fpath).glob("*")]
                    log.info(f"  {sample['Name']} | {tree} | {fpath} is from local path")
            elif self._outputformat == "root":
                for fpath in sample['LocalPath'].split(','):
                    fpath = fpath.strip()
                    self.out_paths_dict[sample['Name']] = [str(Path(f)) for f in Path(fpath).glob("*")]
                    log.info(f"  {sample['Name']} | {fpath} is from local path")


    def write_output_paths_dict(self, out_paths_dict):
        """ Write yaml of output dict """
        if 'WriteOutputDict' in self._config['General'].keys():
            file_out_paths = f"{self.output_path}/{self._config['General']['WriteOutputDict']}.yml"
            with open(file_out_paths, 'w') as f:
                log.debug(f"write a yaml file containg delivered file paths: {f.name}")
                yaml.dump(out_paths_dict, f, default_flow_style=False)
            log.info(f"Wrote a yaml file containing delivered file paths: {file_out_paths}")
        else:
            for yl in list(Path(self.output_path).glob("*yml")):
                Path.unlink(yl)
