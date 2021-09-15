# ServiceX DataBinder

<p align="right"> Release v0.1.2 </p>

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
    GridDID: user.kchoi:user.kchoi.sampleA, 
             user.kchoi:user.kchoi.sampleB
    Tree: nominal
    FuncADL: "Select(lambda event: {'jet_e': event.jet_e, 'jet_pt': event.jet_pt})"
  - Name: ttW
    GridDID: user.kchoi:user.kchoi.sampleC
    Tree: nominal
    Filter: n_jet > 5 
    Columns: jet_e, jet_pt
```

ServiceX query can be constructed with either TCut syntax or func-adl.
- Options for TCut expression:  `Filter` and `Columns`
- Option for Func-adl expression: `FuncADL`

<!-- <sup>1</sup> `Filter` works only for scalar-type of TBranch -->


The following settings are available options:

<!-- `General` block: -->
| Option for `General` | Description       | DataType |
|:--------:|:------:|:------|
| `ServiceXBackendName` | ServiceX backend name (only `uproot` is supported at the moment) | `String` |
| `OutputDirectory` | Path to the directory for ServiceX delivered files | `String` |
| `OutputFormat` | Output file format of ServiceX delivered data (only `parquet` is supported at the moment) | `String` |
| `WriteOutputDict` | Write a yaml file containing Python nested dictionary of output file paths | `String` |
| `IgnoreServiceXCache` | Ignore the existing ServiceX cache and force to make ServiceX requests | `Boolean` |

| Option for `Sample` | Description       |DataType |
|:--------:|:------:|:------|
| `Name`   | sample name defined by a user |`String` |
| `GridDID` | Rucio Dataset Id (DID) for a given sample; Can be multiple DIDs separated by comma |`String` |
| `Tree` | Name of the input ROOT `TTree` |`String` |
| `Filter` | Selection in the TCut syntax, e.g. `jet_pt > 10e3 && jet_eta < 2.0` (TCut ONLY) |`String` |
| `Columns` | List of columns (or branches) to be delivered; multiple columns separately by comma (TCut ONLY) |`String` |
| `FuncADL` | func-adl expression for a given sample (func adl ONLY) |`String` |

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

The function `deliver()` returns a Python nested dictionary: `out['<SAMPLE>']['<TREE>'] = [ List of output files ]`.

## Acknowledgements

Support for this work was provided by the the U.S. Department of Energy, Office of High Energy Physics under Grant No. DE-SC0007890