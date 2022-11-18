# ServiceX DataBinder

<p align="right"> Release v0.3.0 </p>

[![PyPI version](https://badge.fury.io/py/servicex-databinder.svg)](https://badge.fury.io/py/servicex-databinder)

<!-- `servicex-databinder` is a Python package for making multiple ServiceX requests and managing ServiceX delivered data from a configuration file.  -->

`servicex-databinder` is a Python package to interact with ServiceX instance to make ServiceX request(s) and manage ServiceX delivered data efficiently from a single configuration file.

The package is particularly useful when accessing multiple remote data via ServiceX, e.g. full-scale analysis or extracting input data for machine learning.

Brief introduction of ServiceX and walk-though of various DataBinder features can be found 
in this [Jupyter notebook](https://nbviewer.org/github/kyungeonchoi/irishep_topical_ServiceXDataBinder/blob/main/16Nov2022_choi.ipynb).

<!-- [`ServiceX`](https://github.com/ssl-hep/ServiceX) is a scalable HEP event data extraction, transformation and delivery system. 

['ServiceX Client library'](https://github.com/ssl-hep/ServiceX_frontend) provides  --> 

## Prerequisite
- [Access to a ServiceX instance](https://servicex.readthedocs.io/en/latest/user/getting-started/)
- Python 3.6+

## Installation
```shell
pip install servicex-databinder
```

## Configuration file

The configuration file is a yaml file containing all the information.
An example configuration file is shown below:

```yaml
General:
  ServiceXBackendName: uproot
  OutputDirectory: /path/to/output
  OutputFormat: parquet
  
Sample:
  - Name: ttH
    RucioDID: user.kchoi:user.kchoi.sampleA, 
             user.kchoi:user.kchoi.sampleB
    Tree: nominal
    FuncADL: "Select(lambda event: {'jet_e': event.jet_e})"
  - Name: ttW
    XRootDFiles: root://ttW.root
    Tree: nominal
    Filter: n_jet > 5 
    Columns: jet_e, jet_pt
  - Name: ttZ
    LocalPath: /home/kchoi/ttZ
    Tree: nominal
```

`General` block requires three mandatory options as in the example above.

Input dataset for each Sample can be defined either by `RucioDID` or `XRootDFiles` or `LocalPath`. You need to make sure whether the ServiceX backend you specified in `ServiceXBackendName` supports Rucio and/or XRootD. 

ServiceX query can be constructed with either TCut syntax or func-adl.
- Options for TCut syntax: `Filter`<sup>1</sup> and `Columns`
- Option for Func-adl expression: `FuncADL`

&nbsp; &nbsp; &nbsp; <sup>1</sup> `Filter` works only for scalar-type of TBranch.

Output format can be either `Apache parquet` or `ROOT ntuple` for `uproot` backend. Only `ROOT ntuple` format is supported for `xAOD` backend.

Please find other example configurations for ATLAS opendata, xAOD, and Uproot ServiceX endpoints.


The followings are available options:

<!-- `General` block: -->
| Option for `General` block | Description       | DataType |
|:--------:|:------:|:------|
| `ServiceXBackendName`* | ServiceX backend name in your `servicex.yaml` file <br> (name MUST contain either `uproot` or `xaod` to distinguish the type of transformer) | `String` |
| `OutputDirectory`* | Path to the directory for ServiceX delivered files | `String` |
| `OutputFormat`* | Output file format of ServiceX delivered data (`parquet` or `root` for `uproot` / `root` for `xaod`) | `String` |
| `ZipROOTColumns` | Zip columns that share prefix to generate one counter branch (see detail at [uproot readthedoc](https://uproot.readthedocs.io/en/latest/basic.html#writing-ttrees-to-a-file)) | `Boolean` |
| `WriteOutputDict` | Name of an ouput yaml file containing Python nested dictionary of output file paths (located in the `OutputDirectory`) | `String` |
| `IgnoreServiceXCache` | Ignore the existing ServiceX cache and force to make ServiceX requests | `Boolean` |
<p align="right"> *Mandatory options</p>

| Option for `Sample` block | Description       |DataType |
|:--------:|:------:|:------|
| `Name`   | sample name defined by a user |`String` |
| `RucioDID` | Rucio Dataset Id (DID) for a given sample; <br> Can be multiple DIDs separated by comma |`String` |
| `XRootDFiles` | XRootD files (e.g. `root://`) for a given sample; <br> Can be multiple files separated by comma |`String` |
| `Tree` | Name of the input ROOT `TTree`; <br> Can be multiple `TTree`s separated by comma (`uproot` ONLY) |`String` |
| `Filter` | Selection in the TCut syntax, e.g. `jet_pt > 10e3 && jet_eta < 2.0` (TCut ONLY) |`String` |
| `Columns` | List of columns (or branches) to be delivered; multiple columns separately by comma (TCut ONLY) |`String` |
| `FuncADL` | func-adl expression for a given sample (see [example](config_example_xaod.yml)) |`String` |
| `LocalPath` | File path directly from local path (NO ServiceX tranformation) | `String` |

 <!-- Options exclusively for TCut syntax (CANNOT combine with the option `FuncADL`) -->

 <!-- Option for func-adl expression (CANNOT combine with the option `Fitler` and `Columns`) -->

A config file can be simplified by utilizing `Definition` block. You can define placeholders under `Definition` block, which will replace all matched placeholders in the values of `Sample` block. Note that placeholders must start with `DEF_`. The example configuration file can be also written as below with `Definition` block:

```yaml
General:
  ServiceXBackendName: uproot
  OutputDirectory: /path/to/output
  OutputFormat: parquet
  
Sample:
  - Name: ttH
    RucioDID: DEF_ttH_dids
    Tree: nominal
    FuncADL: DEF_nominal_selection
  - Name: ttW
    RucioDID: user.kchoi:user.kchoi.sampleC
    Tree: nominal
    Filter: n_jet > 5 
    Columns: jet_e, jet_pt

Definition:
  DEF_ttH_dids: user.kchoi:user.kchoi.sampleA, 
             user.kchoi:user.kchoi.sampleB
  DEF_nominal_selection: "Select(lambda event: {'jet_e': event.jet_e})"
```

## Deliver data

```python
from servicex_databinder import DataBinder
sx_db = DataBinder('<CONFIG>.yml')
out = sx_db.deliver()
```

The function `deliver()` returns a Python nested dictionary that contains delivered files: 
- for `uproot` backend and `parquet` output format: `out['<SAMPLE>']['<TREE>'] = [ List of output parquet files ]`
- for `uproot` backend and `root` output format: `out['<SAMPLE>'] = [ List of output root files ]`
- for `xAOD` backend: `out['<SAMPLE>'] = [ List of output root files ]`

Input configuration can be also passed in a form of a Python dictionary.

Delivered Samples and files in the `OutputDirectory` are always synced with the DataBinder config file.

<!-- ## Currently available 
- Dataset as Rucio DID + Input file format is ROOT TTree + ServiceX delivers output in parquet format
- Dataset as Rucio DID + Input file format is ATLAS xAOD + ServiceX delivers output in ROOT TTree format
- Dataset as XRootD + Input file format is ROOT TTree + ServiceX delivers output in parquet format -->

## Error handling

```python
failed_requests = sx_db.get_failed_requests()
```

If failed ServiceX request(s), `deliver()` will print number of failed requests and the name of Sample, Tree if present, and input dataset. You can get a full list of failed samples and error messages for each by `get_failed_requests()` function. If it is not clear from the message you can browse `Logs` in the ServiceX instance webpage for the detail.

## Useful tools

### Create Rucio container for multiple DIDs

The current ServiceX generates one request per Rucio DID. 
It's often the case that a physics analysis needs to process hundreds of DIDs.
In such cases, the script (`scripts/create_rucio_container.py`) can be used to create one Rucio container per Sample from a yaml file.
An example yaml file (`scripts/rucio_dids_example.yaml`) is included.

Here is the usage of the script:

```shell
usage: create_rucio_containers.py [-h] [--dry-run DRY_RUN]
                                  infile container_name version

Create Rucio containers from multiple DIDs

positional arguments:
  infile             yaml file contains Rucio DIDs for each Sample
  container_name     e.g. user.kchoi:user.kchoi.<container-name>.Sample.v1
  version            e.g. user.kchoi:user.kchoi.fcnc_ana.Sample.<version>

optional arguments:
  -h, --help         show this help message and exit
  --dry-run DRY_RUN  Run without creating new Rucio container

```

## Acknowledgements

Support for this work was provided by the the U.S. Department of Energy, Office of High Energy Physics under Grant No. DE-SC0007890