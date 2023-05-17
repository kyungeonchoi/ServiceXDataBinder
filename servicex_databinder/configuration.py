from servicex import servicex_config
import yaml
import pathlib
from typing import Any, Dict, Union
import logging
log = logging.getLogger(__name__)


def LoadConfig(input_config:
               Union[str, pathlib.Path, Dict[str, Any]]
               ) -> Dict[str, Any]:
    """Loads, validates, and returns a config file from the provided path.
    Args:
        input_config (Union[str, pathlib.Path, Dict[str, Any]]):
            path to config file or config as dict
    Returns:
        Dict[str, Any]: configuration
    """

    if isinstance(input_config, dict):
        _replace_definition_in_sample_block(input_config)
        _validate_config(input_config)
        return _update_backend_per_sample(input_config)
    else:
        file_path = pathlib.Path(input_config)
        log.info(f"Loading DataBinder config file: {file_path}")
        try:
            config = yaml.safe_load(file_path.read_text())
            _replace_definition_in_sample_block(config)
            _validate_config(config)
            return _update_backend_per_sample(config)
        except Exception:
            raise FileNotFoundError(
                f"Exception occured while reading config file: {file_path}"
                )


def _replace_definition_in_sample_block(config: Dict[str, Any]):
    flag = False
    if config.get('Definition'):
        for n, sample in enumerate(config.get('Sample')):
            for field, value in sample.items():
                if 'DEF_' in value:
                    for repre, new_str in config.get('Definition').items():
                        if repre in value:
                            log.debug(
                                f"Replace Definition for {sample['Name']} "
                                f"- {field}: {repre} with {new_str}")
                            config.get('Sample')[n][field] \
                                = config.get('Sample')[n][field] \
                                        .replace(repre, new_str)
                            flag = True
        return flag
    else:
        return flag


def _validate_config(config: Dict[str, Any]) -> bool:
    """Returns True if the config file is validated,
    otherwise raises exceptions.
    Checks that the config satisfies the json schema,
    and performs additional checks to
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

    available_keys = [
        'General', 'ServiceXName', 'OutputDirectory', 'Transformer',
        'OutputFormat', 'WriteOutputDict', 'Name',
        'IgnoreLocalCache', 'Sample', 'RucioDID', 'XRootDFiles', 'Tree',
        'Filter', 'Columns', 'FuncADL', 'LocalPath', 'Definition',
        'ServiceXBackendName', 'IgnoreServiceXCache'
        ]

    if 'General' not in config.keys() and 'Sample' not in config.keys():
        raise KeyError("You should have 'General' block and "
                       "at least one 'Sample' block in the config")

    keys_in_config = set()

    for item in config['General']:
        keys_in_config.add(item)
    for sample in config['Sample']:
        for item in sample.keys():
            keys_in_config.add(item)
    for key in keys_in_config:
        if key not in available_keys:
            raise KeyError(f"Unknown Option {key} in the config")

    if ('ServiceXName' not in config['General'].keys()) and \
            ('ServiceXBackendName' not in config['General'].keys()):
        raise KeyError("Option 'ServiceXName' is required in General block")

    # if 'Transformer' not in config['General'].keys():
    #     raise KeyError("Option 'Transformer' is required in General block")

    # if 'OutputDirectory' not in config['General'].keys():
    #     raise KeyError("OutputDirectory is required")

    if 'OutputFormat' not in config['General'].keys():
        raise KeyError("OutputFormat is required")
    elif config['General']['OutputFormat'].lower() != 'parquet' and \
            config['General']['OutputFormat'].lower() != 'root':
        raise ValueError("OutputFormat can be either parquet or root")

    for sample in config['Sample']:
        if ('RucioDID' not in sample.keys()) \
                and ('XRootDFiles' not in sample.keys()) \
                and ('LocalPath' not in sample.keys()):
            raise KeyError(
                "Please specify a valid input option "
                f"for Sample {sample['Name']} e.g. RucioDID"
                )
        if 'RucioDID' in sample.keys():
            for did in sample['RucioDID'].split(","):
                if len(did.split(":")) != 2:
                    raise ValueError(
                        f"Sample {sample['Name']} "
                        f"- RucioDID {did} is missing the scope"
                        )
        # if ('Tree' in sample) and \
        #         ('uproot' not in config['General']['ServiceXName'].lower()):
        #     raise KeyError(
        #         f"Tree in Sample {sample['Name']} "
        #         "is only for uproot backend type"
        #         )
        if 'Columns' in sample and 'FuncADL' in sample:
            raise KeyError(
                f"Sample {sample['Name']} - Use one type of query per sample: "
                "Columns for TCut and FuncADL for func-adl"
                )
        if 'FuncADL' in sample and 'Filter' in sample:
            raise KeyError(
                f"Sample {sample['Name']} - "
                "You cannot use Filter with func-adl query"
                )

    log.debug("config looks okay")
    return True


def _update_backend_per_sample(config: Dict[str, Any]) -> Dict:
    """ from servicex.yaml file """
    backend_type = servicex_config.ServiceXConfigAdaptor()\
        .get_backend_info(config['General']['ServiceXName'], "type")
    if backend_type == "xaod":
        pair = ("xaod", "atlasr21")
    elif backend_type == "uproot":
        pair = ("uproot", "uproot")

    """ from General block """
    if 'Transformer' in config['General'].keys():
        if config['General']['Transformer'] == "atlasr21":
            pair = ("xaod", "atlasr21")
        elif config['General']['Transformer'] == "uproot":
            pair = ("uproot", "uproot")
        elif config['General']['Transformer'] == "python":
            pair = ("uproot", "python")

    """ from Sample block """
    for (idx, sample) in zip(range(len(config['Sample'])), config['Sample']):
        if 'Transformer' in sample.keys():
            if sample['Transformer'] == "atlasr21":
                config['Sample'][idx]['Type'] = "xaod"
            elif sample['Transformer'] == "uproot":
                config['Sample'][idx]['Type'] = "uproot"
            elif sample['Transformer'] == "python":
                config['Sample'][idx]['Type'] = "uproot"
        else:
            config['Sample'][idx]['Type'] = pair[0]
            config['Sample'][idx]['Transformer'] = pair[1]

    return config
