import yaml
import pathlib
from typing import Any, Dict, Union
import logging
log = logging.getLogger(__name__)

def _load_config(input_config: Union[str, pathlib.Path, Dict[str, Any]]) -> Dict[str, Any]:
    """Loads, validates, and returns a config file from the provided path.
    Args:
        input_config (Union[str, pathlib.Path, Dict[str, Any]]): path to config file or config as dict 
    Returns:
        Dict[str, Any]: configuration
    """
    
    if isinstance(input_config, dict):
        _replace_definition_in_sample_block(input_config)
        _validate_config(input_config)
        return input_config
    else:
        file_path = pathlib.Path(input_config)
        log.info(f"Loading DataBinder config file: {file_path}")
        try:
            config = yaml.safe_load(file_path.read_text())
            _replace_definition_in_sample_block(config)
            _validate_config(config)
            return config
        except:
            raise FileNotFoundError(f"Exception occured while reading config file: {file_path}")


def _replace_definition_in_sample_block(config: Dict[str, Any]):
    flag = False
    if config.get('Definition'):
        for n, sample in enumerate(config.get('Sample')):
            for field, value in sample.items():
                if 'DEF_' in value:
                    for repre, new_str in config.get('Definition').items():
                        if repre in value:
                            log.debug(f"Replace Definition for {sample['Name']} - {field}: {repre} with {new_str}")
                            config.get('Sample')[n][field] = config.get('Sample')[n][field].replace(repre, new_str)
                            flag = True
        return flag
    else:
        return flag


def _validate_config(config: Dict[str, Any]) -> bool:
    """Returns True if the config file is validated, otherwise raises exceptions.
    Checks that the config satisfies the json schema, and performs additional checks to
    validate the config further.
    Args:
        config (Dict[str, Any]): configuration
    Raises:
        NotImplementedError
        ValueError
        KeyError
    Returns:
        bool: whether the validation was successful
    """
    
    if 'General' not in config.keys() and 'Sample' not in config.keys():
        raise KeyError(f"You should have 'General' and 'Sample' in the config")
    
    if 'ServiceXBackendName' not in config['General'].keys():
        raise KeyError(f"ServiceXBackendName is required")
    elif 'uproot' not in config['General']['ServiceXBackendName'].lower() and \
         'xaod' not in config['General']['ServiceXBackendName'].lower():
        raise ValueError(f"ServiceXBackendName should contain either uproot or xaod")

    if 'OutputDirectory' not in config['General'].keys():
        raise KeyError(f"OutputDirectory is required")

    if 'OutputFormat' not in config['General'].keys():
        raise KeyError(f"OutputFormat is required")
    elif config['General']['OutputFormat'].lower() != 'parquet' and \
         config['General']['OutputFormat'].lower() != 'root':
        raise ValueError(f"OutputFormat can be either parquet or root")

    for sample in config['Sample']:        
        if 'RucioDID' not in sample.keys() and 'XRootDFiles' not in sample.keys() and 'LocalPath' not in sample.keys():
            raise KeyError(f"Please specify a valid input option for Sample {sample['Name']} e.g. RucioDID")
        if 'RucioDID' in sample.keys():
            for did in sample['RucioDID'].split(","):
                if len(did.split(":")) != 2:
                    raise ValueError(f"Sample {sample['Name']} - RucioDID {did} is missing the scope")
        if 'Tree' in sample and 'uproot' not in config['General']['ServiceXBackendName'].lower():
            raise KeyError(f"Tree in Sample {sample['Name']} is only for uproot backend type")
        if 'Columns' in sample and 'FuncADL' in sample:
            raise KeyError(f"Sample {sample['Name']} - Use one type of query per sample: Columns for TCut and FuncADL for func-adl")
        if 'FuncADL' in sample and 'Filter' in sample:
            raise KeyError(f"Sample {sample['Name']} - You cannot use Filter with func-adl query")

    log.debug("config looks okay")
    return True