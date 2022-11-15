import yaml
from pathlib import Path
from typing import Any, Dict
import time
from shutil import rmtree

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

        """Prepare output path dictionary"""        
        out_paths = {}    
        samples = []
        [samples.append(sample['Name']) for sample in config['Sample'] if sample['Name'] not in samples]
        for sample in samples:
            out_paths[sample] = {}
        if self._backend == "uproot":
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

    
    def update_output_paths_dict(self, req, files, format:str = "parquet"):
        if self._backend == "uproot":
            # Outfile paths dictionary - add files based on the returned file list from ServiceX
            target_path = Path(self.output_path, req['Sample'], req['tree'])
            paths_in_output_dict = self.out_paths_dict[req['Sample']][req['tree']]
            if format == "parquet":
                new_files = [str(Path(target_path, Path(file).name)) for file in files]
            elif format == "root":
                new_files = [str(Path(target_path, Path(file).name).with_suffix('.root')) for file in files]

            if paths_in_output_dict:
                output_dict = list(set(paths_in_output_dict + new_files))
            else:
                output_dict = new_files

            self.out_paths_dict[req['Sample']][req['tree']] = output_dict
        elif self._backend == "xaod":
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
            if self._backend == "uproot":
                for tree, fpath in zip(sample['Tree'].split(','), sample['LocalPath'].split(',')):
                    tree = tree.strip()
                    fpath = fpath.strip()
                    self.out_paths_dict[sample['Name']][tree] = [str(Path(f)) for f in Path(fpath).glob("*")]
                    log.info(f"  {sample['Name']} | {tree} | {fpath} is from local path")
            elif self._backend == "xaod":
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


    def clean_up_files_not_in_requests(self, out_paths_dict):

        samples_in_requests = out_paths_dict.keys()
        samples_local = [sa.name for sa in self.output_path.iterdir() if sa.is_dir()]

        if self._backend == "uproot":
            for sample in samples_local:
                if not sample in samples_in_requests:
                    rmtree(Path(self.output_path, sample))
                else:
                    # for tree in out_paths_dict[sample].keys():
                    for tree in [tr.name for tr in Path(self.output_path, sample).iterdir() if tr.is_dir()]:
                        if tree in out_paths_dict[sample].keys():
                            files_local = set(Path(self.output_path, sample, tree).glob("*"))
                            files_request = set([Path(item) for item in out_paths_dict[sample][tree]])
                            for tbd in files_local.difference(files_request):
                                Path.unlink(tbd)
                        else:
                            rmtree(Path(self.output_path, sample, tree))
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