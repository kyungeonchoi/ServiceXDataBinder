import yaml
from pathlib import Path
from typing import Any, Dict

from .configuration import _load_config

import logging
log = logging.getLogger(__name__)

class OutputHandler:

    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = _load_config(config)

        """
        Prepare output path dictionary
        """
        out_paths = {}    
        samples = []
        [samples.append(sample['Name']) for sample in config['Sample'] if sample['Name'] not in samples]
        for sample in samples:
            out_paths[sample] = {}
        if "uproot" in config['General']['ServiceXBackendName'].lower():
            for sample in config['Sample']:
                for tree in sample['Tree'].split(','):
                    out_paths[sample['Name']][tree.strip()] = []
        self.out_paths_dict = out_paths


    def get_outpath(self):
        if 'OutputDirectory' in self._config['General'].keys():
            self.output_path = Path(self._config['General']['OutputDirectory']).absolute()
            self.output_path.mkdir(parents=True, exist_ok=True)
            return self.output_path
        else:
            self.output_path = Path('ServiceXData').absolute()
            self.output_path.mkdir(parents=True, exist_ok=True)
            return self.output_path


    def write_output_paths_dict(self, out_paths_dict):
        """ Write yaml of output dict """
        if 'WriteOutputDict' in self._config['General'].keys():
            file_out_paths = f"{self.output_path}/{self._config['General']['WriteOutputDict']}.yml"
            with open(file_out_paths, 'w') as f:
                log.debug(f"write a yaml file containg delivered file paths: {f.name}")
                yaml.dump(out_paths_dict, f, default_flow_style=False)
            log.info(f"Wrote a file containing delivered file paths: {file_out_paths}")
        else:
            for yl in list(Path(self.output_path).glob("*yml")):
                Path.unlink(yl)