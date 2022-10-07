# ServiceX DataBinder

<p align="right"> Release v0.2.10 </p>

[![PyPI version](https://badge.fury.io/py/servicex-databinder.svg)](https://badge.fury.io/py/servicex-databinder)

ServiceX DataBinder is a Python package for making multiple ServiceX requests and managing ServiceX delivered data from a configuration file. 

<!-- [`ServiceX`](https://github.com/ssl-hep/ServiceX) is a scalable HEP event data extraction, transformation and delivery system. 

['ServiceX Client library'](https://github.com/ssl-hep/ServiceX_frontend) provides  --> 

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
    RucioDID: user.kchoi:user.kchoi.sampleC
    Tree: nominal
    Filter: n_jet > 5 
    Columns: jet_e, jet_pt
```

Input dataset can be defined either by `RucioDID` or `XRootDFiles`. You need to make sure whether the ServiceX backend you specified in `ServiceXBackendName` supports Rucio and/or XRootD. 

ServiceX query can be constructed with either TCut syntax or func-adl.
- Options for TCut syntax: `Filter`<sup>1</sup> and `Columns`
- Option for Func-adl expression: `FuncADL`

&nbsp; &nbsp; &nbsp; <sup>1</sup> `Filter` works only for scalar-type of TBranch.

Output format can be either `Apache parquet` or `ROOT ntuple` for `uproot` backend. Only `ROOT ntuple` format is supported for `xAOD` backend.

Please find other example configurations for ATLAS opendata, xAOD, and Uproot ServiceX endpoints.


The followings are available options:

<!-- `General` block: -->
| Option for `General` | Description       | DataType |
|:--------:|:------:|:------|
| `ServiceXBackendName` | ServiceX backend name in your `servicex.yaml` file <br> (name should contain either `uproot` or `xAOD` to distinguish the type of transformer) | `String` |
| `OutputDirectory` | Path to the directory for ServiceX delivered files | `String` |
| `OutputFormat` | Output file format of ServiceX delivered data (`parquet` or `root` for `uproot` / `root` for `xaod`) | `String` |
| `ZipROOTColumns` | Zip columns that share prefix to generate one counter branch (see detail at [uproot readthedoc](https://uproot.readthedocs.io/en/latest/basic.html#writing-ttrees-to-a-file)) | `Boolean` |
| `WriteOutputDict` | Name of an ouput yaml file containing Python nested dictionary of output file paths (located in the `OutputDirectory`) | `String` |
| `IgnoreServiceXCache` | Ignore the existing ServiceX cache and force to make ServiceX requests | `Boolean` |

| Option for `Sample` | Description       |DataType |
|:--------:|:------:|:------|
| `Name`   | sample name defined by a user |`String` |
| `RucioDID` | Rucio Dataset Id (DID) for a given sample; <br> Can be multiple DIDs separated by comma |`String` |
| `XRootDFiles` | XRootD files (e.g. `root://`) for a given sample; <br> Can be multiple files separated by comma |`String` |
| `Tree` | Name of the input ROOT `TTree`; <br> Can be multiple `TTree`s separated by comma (`uproot` ONLY) |`String` |
| `Filter` | Selection in the TCut syntax, e.g. `jet_pt > 10e3 && jet_eta < 2.0` (TCut ONLY) |`String` |
| `Columns` | List of columns (or branches) to be delivered; multiple columns separately by comma (TCut ONLY) |`String` |
| `FuncADL` | func-adl expression for a given sample (see [example](config_example_xaod.yml)) |`String` |

 <!-- Options exclusively for TCut syntax (CANNOT combine with the option `FuncADL`) -->

 <!-- Option for func-adl expression (CANNOT combine with the option `Fitler` and `Columns`) -->

<!-- ## Installation

```python
pip -m install servicex_databinder
``` -->

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

Input configuration can be also feed as a dictionary.

<!-- ## Currently available 
- Dataset as Rucio DID + Input file format is ROOT TTree + ServiceX delivers output in parquet format
- Dataset as Rucio DID + Input file format is ATLAS xAOD + ServiceX delivers output in ROOT TTree format
- Dataset as XRootD + Input file format is ROOT TTree + ServiceX delivers output in parquet format -->

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