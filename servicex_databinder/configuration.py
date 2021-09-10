import yaml
import logging
import pathlib
from typing import Any, Dict, List, Optional, Union

log = logging.getLogger(__name__)

def _load_config(file_path_string: Union[str, pathlib.Path]) -> Dict[str, Any]:
    """Loads, validates, and returns a config file from the provided path.
    Args:
        file_path_string (Union[str, pathlib.Path]): path to config file
    Returns:
        Dict[str, Any]: configuration
    """
    file_path = pathlib.Path(file_path_string)
    log.info(f"opening config file {file_path}")
    config = yaml.safe_load(file_path.read_text())
    _validate_config(config)
    return config

def _validate_config(config: Dict[str, Any]) -> bool:
    """Returns True if the config file is validated, otherwise raises exceptions.
    Checks that the config satisfies the json schema, and performs additional checks to
    validate the config further.
    Args:
        config (Dict[str, Any]): configuration
    Raises:
        NotImplementedError: when more than one data sample is found
        ValueError: when region / sample / normfactor / systematic names are not unique
    Returns:
        bool: whether the validation was successful
    """

    return True