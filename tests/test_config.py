from unittest import mock
import pytest

from servicex_databinder import configuration

@mock.patch("servicex_databinder.configuration._validate_config")
def test_load_config(mock_validation):
    conf = configuration._load_config("config_example_uproot.yml")
    assert isinstance(conf, dict)
    mock_validation.assert_called_once()

    with pytest.raises(FileNotFoundError):
        conf = configuration._load_config("none.yml")

def test_validate_config():
    config_without_general = {}
    with pytest.raises(KeyError):
        configuration._validate_config(config_without_general)

    config_without_servicexbackendname = {"General": {}, 
                            "Sample": [{},]}
    with pytest.raises(KeyError):
        configuration._validate_config(config_without_servicexbackendname)

    config_without_wrong_backendname = {
        "General": {
            "ServiceXBackendName": "wrong_backend_name",
        },
        "Sample": [{},]}
    with pytest.raises(ValueError):
        configuration._validate_config(config_without_wrong_backendname)
    
    config_without_outputdirectory = {
        "General": {
            "ServiceXBackendName": "uproot",
        },
        "Sample": [{},]}
    with pytest.raises(KeyError):
        configuration._validate_config(config_without_outputdirectory)

    config_without_outputformat = {
        "General": {
            "ServiceXBackendName": "uproot",
            "OutputDirectory": "a",
        },
        "Sample": [{},]}
    with pytest.raises(KeyError):
        configuration._validate_config(config_without_outputformat)

    config_wrong_outputformat = {
        "General": {
            "ServiceXBackendName": "uproot",
            "OutputDirectory": "a",
            "OutputFormat": "pandas",
        },
        "Sample": [{},]}
    with pytest.raises(ValueError):
        configuration._validate_config(config_wrong_outputformat)

    config_without_rucio_did = {
        "General": {
            "ServiceXBackendName": "uproot",
            "OutputDirectory": "a",
            "OutputFormat": "parquet",
        },
        "Sample": [{
            "Name": "ttH",
        }]
    }
    with pytest.raises(KeyError):
        configuration._validate_config(config_without_rucio_did)

    config_without_rucio_scope = {
        "General": {
            "ServiceXBackendName": "uproot",
            "OutputDirectory": "a",
            "OutputFormat": "parquet",
        },
        "Sample": [{
            "Name": "ttH",
            "RucioDID": "user.kchoi:user.kchoi.A, user.kchoi.B",
        }]
    }
    with pytest.raises(ValueError):
        configuration._validate_config(config_without_rucio_scope)

    config_tree_not_with_uproot = {
        "General": {
            "ServiceXBackendName": "xaod",
            "OutputDirectory": "a",
            "OutputFormat": "root",
        },
        "Sample": [{
            "Name": "ttH",
            "RucioDID": "user.kchoi:user.kchoi.A, user.kchoi:user.kchoi.B",
            "Tree": "nominal",
        }]
    }
    with pytest.raises(KeyError):
        configuration._validate_config(config_tree_not_with_uproot)

    config_columns_with_funcadl = {
        "General": {
            "ServiceXBackendName": "xaod",
            "OutputDirectory": "a",
            "OutputFormat": "root",
        },
        "Sample": [{
            "Name": "ttH",
            "RucioDID": "user.kchoi:user.kchoi.A, user.kchoi:user.kchoi.B",
            "Columns": "jet_pt",
            "FuncADL": "Select()",
        }]
    }
    with pytest.raises(KeyError):
        configuration._validate_config(config_columns_with_funcadl)

    config_filter_with_funcadl = {
        "General": {
            "ServiceXBackendName": "xaod",
            "OutputDirectory": "a",
            "OutputFormat": "root",
        },
        "Sample": [{
            "Name": "ttH",
            "RucioDID": "user.kchoi:user.kchoi.A, user.kchoi:user.kchoi.B",
            "Filter": "jet_pt>15e3",
            "FuncADL": "Select()",
        }]
    }
    with pytest.raises(KeyError):
        configuration._validate_config(config_filter_with_funcadl) 

    config_valid_uproot ={
        "General": {
            "ServiceXBackendName": "uproot",
            "OutputDirectory": "a",
            "OutputFormat": "parquet",
        },
        "Sample": [{
            "Name": "ttH",
            "RucioDID": "user.kchoi:user.kchoi.A, user.kchoi:user.kchoi.B",
            "FuncADL": "Select()",
        }]
    }
    assert configuration._validate_config(config_valid_uproot)




# def test_validate_config_for_uproot():
#     config_valid_uproot ={
#         "General": {
#             "ServiceXBackendName": "uproot_test",
#             "OutputDirectory": "a",
#             "OutputFormat": "parquet",
#         },
#         "Sample": [{
#             "Name": "ttH",
#             "RucioDID": "user.kchoi:user.kchoi",
#             "FuncADL": "Select()",
#         }]
#     }
#     assert configuration._validate_config(config_valid_uproot)

# def test_validate_config_for_xaod():
#     config_valid_xaod ={
#         "General": {
#             "ServiceXBackendName": "xaod_test",
#             "OutputDirectory": "a",
#             "OutputFormat": "root",
#         },
#         "Sample": [{
#             "Name": "ttH",
#             "RucioDID": "user.kchoi:user.kchoi",
#             "FuncADL": 1,
#         }]
#     }
#     assert configuration._validate_config(config_valid_xaod)