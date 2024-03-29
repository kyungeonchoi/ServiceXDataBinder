import yaml
from pathlib import Path
from typing import Any, Dict
from shutil import rmtree, copy

import pyarrow.parquet as pq
import awkward as ak
import uproot

import logging
log = logging.getLogger(__name__)


class OutputHandler():

    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = config
        self._outputformat \
            = self._config.get('General')['OutputFormat'].lower()
        """
        Prepare output path dictionary
        """
        out_paths = {}
        samples = []
        [samples.append(sample['Name'])
            for sample in config['Sample'] if sample['Name'] not in samples]
        for sample in samples:
            out_paths[sample] = {}
        for sample in config['Sample']:
            # if sample['Transformer'] == "uproot":
            if 'Tree' in sample.keys():
                for tree in sample['Tree'].split(','):
                    out_paths[sample['Name']][tree.strip()] = []

        self.out_paths_dict = out_paths

        """
        Create output directory
        """
        if 'OutputDirectory' in self._config['General'].keys():
            self.output_path = Path(
                self._config['General']['OutputDirectory']
                ).absolute()
            self.output_path.mkdir(parents=True, exist_ok=True)
        else:
            self.output_path = Path('ServiceXData').absolute()
            self.output_path.mkdir(parents=True, exist_ok=True)

    def copy_to_target(self, delivery_setting, req, files):
        if req['codegen'] == "uproot":
            target_path = Path(self.output_path, req['Sample'], req['tree'])
            delivery_info = (f"  {req['Sample']} | "
                             f"{req['tree']} | "
                             f"{str(req['dataset'])[:100]}")
        elif req['codegen'] == "atlasr21" or req['codegen'] == "python":
            target_path = Path(self.output_path, req['Sample'])
            delivery_info = (f"  {req['Sample']} | "
                             f"{str(req['dataset'])[:100]}")

        if delivery_setting == 1 or delivery_setting == 2:
            if target_path.exists():
                servicex_files = {Path(file).name for file in files}
                local_files = {
                    Path(file).name for file in list(target_path.glob("*"))
                    }
                servicex_data_path = Path(files[0]).parents[0]
                # one RucioDID for this sample and files are already there
                if servicex_files == local_files:
                    log.info(f"{delivery_info} is already delivered")
                else:
                    # copy files in servicex but not in local
                    files_not_in_local = servicex_files.difference(local_files)
                    if files_not_in_local:
                        for file in files_not_in_local:
                            copy(Path(servicex_data_path, file),
                                 Path(target_path, file))
                        log.info(f"{delivery_info} is delivered")
                    else:
                        log.info(f"{delivery_info} is already delivered")
            else:
                target_path.mkdir(parents=True, exist_ok=True)
                for file in files:
                    outfile = Path(target_path, Path(file).name)
                    copy(file, outfile)
                log.info(f"{delivery_info} is delivered")
        elif delivery_setting == 3 or delivery_setting == 4:
            log.info(f"{delivery_info} is cached locally")
        elif delivery_setting == 5 or delivery_setting == 6:
            log.info(f"{delivery_info} is available at the object store")

    def parquet_to_root(self, tree_name, pq_file, root_file):
        """
        Write ROOT ntuple from parquet file
        """
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

    def update_output_paths_dict(
            self,
            req,
            files,
            delivery_setting: int
            ):
        """
        Update dictionary of outfile paths
        """
        if req['codegen'] == "uproot":
            target_path = Path(self.output_path, req['Sample'], req['tree'])
            paths_in_output_dict = \
                self.out_paths_dict[req['Sample']][req['tree']]
        elif req['codegen'] == "atlasr21" or req['codegen'] == "python":
            target_path = Path(self.output_path, req['Sample'])
            paths_in_output_dict = self.out_paths_dict[req['Sample']]

        # Update file path if deliver to localpath
        if delivery_setting == 1 or delivery_setting == 2:
            new_files = [
                str(Path(target_path, Path(file).name))
                for file in files
                ]
        elif delivery_setting == 5 or delivery_setting == 6:
            new_files = [file._url for file in files]
        else:
            new_files = [str(file) for file in files]

        # Update output_dict
        if paths_in_output_dict:
            output_dict = list(set(paths_in_output_dict + new_files))
        else:
            output_dict = new_files

        if req['codegen'] == "uproot":
            self.out_paths_dict[req['Sample']][req['tree']] = output_dict
        elif req['codegen'] == "atlasr21" or req['codegen'] == "python":
            self.out_paths_dict[req['Sample']] = output_dict

    def add_local_output_paths_dict(self):
        local_samples = [sample for sample in self._config.get('Sample')
                         if 'LocalPath' in sample.keys()]
        for sample in local_samples:
            if 'Tree' in sample.keys():
                for tree, fpath in zip(sample['Tree'].split(','),
                                       sample['LocalPath'].split(',')):
                    tree = tree.strip()
                    fpath = fpath.strip()
                    self.out_paths_dict[sample['Name']][tree] \
                        = [str(Path(f)) for f in Path(fpath).glob("*")]
                    log.info(f"  {sample['Name']} "
                             f"| {tree} | {fpath} is from local path")
            else:
                for fpath in sample['LocalPath'].split(','):
                    fpath = fpath.strip()
                    self.out_paths_dict[sample['Name']] = \
                        [str(Path(f)) for f in Path(fpath).glob("*")]
                    log.info(f"  {sample['Name']} "
                             f"| {fpath} is from local path")

    def write_output_paths_dict(self, out_paths_dict):
        """
        Write yaml of output paths
        """
        if 'WriteOutputDict' in self._config['General'].keys():
            file_out_paths = \
                (f"{self.output_path}/"
                 f"{self._config['General']['WriteOutputDict']}.yml")
            with open(file_out_paths, 'w') as f:
                log.debug("YAML file containing delivered file paths: "
                          f"{f.name}")
                yaml.dump(out_paths_dict, f, default_flow_style=False)
            log.info("YAML file containing delivered file paths: "
                     f"{file_out_paths}")
        else:
            for yl in list(Path(self.output_path).glob("*yml")):
                Path.unlink(yl)

    def clean_up_files_not_in_requests(self, out_paths_dict):

        samples_in_requests = list(out_paths_dict.keys())
        samples_local = [sa.name for sa in self.output_path.iterdir()
                         if sa.is_dir()]
        for sample in samples_local:
            if not (sample in samples_in_requests):
                rmtree(Path(self.output_path, sample))
            else:
                if list(Path(self.output_path, sample).iterdir())[0].is_dir():
                    for tree in out_paths_dict[sample].keys():
                        for tree in [tr.name for tr in
                                     Path(self.output_path, sample).iterdir()
                                     if tr.is_dir()]:
                            if tree in out_paths_dict[sample].keys():
                                files_local = set(Path(self.output_path,
                                                       sample, tree).glob("*"))
                                files_request = set(
                                    [Path(item)
                                     for item in out_paths_dict[sample][tree]]
                                    )
                                for tbd in \
                                        files_local.difference(files_request):
                                    Path.unlink(tbd)
                            else:
                                rmtree(Path(self.output_path, sample, tree))
                else:
                    files_local = set(Path(self.output_path, sample).glob("*"))
                    files_request = set(
                        [Path(item) for item in out_paths_dict[sample]]
                        )
                    for tbd in files_local.difference(files_request):
                        Path.unlink(tbd)

        return
