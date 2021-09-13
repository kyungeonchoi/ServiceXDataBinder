# ServiceX DataBinder

ServiceX DataBinder is a Python package for making multiple ServiceX requests and managing ServiceX delivered data from a configuration file. 

<!-- [`ServiceX`](https://github.com/ssl-hep/ServiceX) is a scalable HEP event data extraction, transformation and delivery system. 

['ServiceX Client library'](https://github.com/ssl-hep/ServiceX_frontend) provides  -->

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

The following settings are available options:

<!-- `General` block: -->
| Option for `General` | Description       |
|:--------:|:------:|
| `ServiceXBackendName` | ServiceX backend name (only `uproot` is supported at the moment) |
| `OutputDirectory` | Path to the directory for ServiceX delivered files |
| `OutputFormat` | Output file format of ServiceX delivered data (only `parquet` is supported at the moment) |
| `IgnoreServiceXCache` | Ignore the existing ServiceX cache and force to make ServiceX requests |

| Option for `Sample` | Description       |
|:--------:|:------:|
| `Name`   | sample name defined by a user |
| `GridDID` | Rucio Dataset Id (DID) for a given sample; Can be multiple DIDs separated by comma |
| `Tree` | Name of the input ROOT `TTree` |
| `Filter`<sup>1</sup> | Selection in the TCut syntax, e.g. `jet_pt > 10e3 && jet_eta < 2.0`  |
| `Columns`<sup>1</sup> | List of columns (or branches) to be delivered; multiple columns separately by comma |
| `FuncADL`<sup>2</sup> | func-adl expression for a given sample |

<sup>1</sup> Options for TCut syntax (CANNOT combine with the option `FuncADL`)

<sup>2</sup> Option for func-adl expression (CANNOT combine with the option `Fitler` and `Columns`)

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

## Acknowledgements

Support for this work was provided by the the U.S. Department of Energy, Office of High Energy Physics under Grant No. DE-SC0007890