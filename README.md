# ServiceX DataBinder

<p align="right"> Release v0.4.1 </p>

[![PyPI version](https://badge.fury.io/py/servicex-databinder.svg)](https://badge.fury.io/py/servicex-databinder)

`servicex-databinder` is a user-analysis data management package using a single configuration file. 
Samples with external data sources (e.g. `RucioDID` or `XRootDFiles`) utilize ServiceX to deliver user-selected columns with optional row filtering.
<!-- to interact with ServiceX instance to make ServiceX request(s) and manage ServiceX delivered data from a single configuration file. -->

The following table shows supported ServiceX transformers by DataBinder

| Input format | Code generator | Transformer | Output format
| :--- | :---: | :---: | :---: |
| ROOT Ntuple | func-adl | `uproot` | `root` or `parquet` |
| ATLAS Release 21 xAOD | func-adl | `atlasr21`| `root` |
<!-- | ROOT Ntuple | python function | `python`| -->

<!-- [`ServiceX`](https://github.com/ssl-hep/ServiceX) is a scalable HEP event data extraction, transformation and delivery system. 

['ServiceX Client library'](https://github.com/ssl-hep/ServiceX_frontend) provides  --> 

## Prerequisite
- [Access to a ServiceX instance](https://servicex.readthedocs.io/en/latest/user/getting-started/)
- Python 3.7+

## Installation
```shell
pip install servicex-databinder
```

## Configuration file

The configuration file is a yaml file containing all the information.

The [following example configuration file](config_minimum.yaml) contains minimal fields. You can also download [`servicex-opendata.yaml`](servicex-opendata.yaml) file (rename to `servicex.yaml`) at your working directory, and run DataBinder for OpenData without an access token.

```yaml
General:
  ServiceXName: servicex-opendata
  OutputFormat: parquet
  
Sample:
  - Name: ggH125_ZZ4lep
    XRootDFiles: "root://eospublic.cern.ch//eos/opendata/atlas/OutreachDatasets\
                  /2020-01-22/4lep/MC/mc_345060.ggH125_ZZ4lep.4lep.root"
    Tree: mini
    Columns: lep_pt, lep_eta
```

`General` block requires two mandatory options (`ServiceXName` and `OutputFormat`) as in the example above.

Input dataset for each Sample can be defined either by `RucioDID` or `XRootDFiles` or `LocalPath`.

ServiceX query can be constructed with either TCut syntax or func-adl.
- Options for TCut syntax: `Filter`<sup>1</sup> and `Columns`
- Option for Func-adl expression: `FuncADL`

&nbsp; &nbsp; &nbsp; <sup>1</sup> `Filter` works only for scalar-type of TBranch.

Output format can be either `Apache parquet` or `ROOT ntuple` for `uproot` backend. Only `ROOT ntuple` format is supported for `xAOD` backend.


The followings are available options:

<!-- `General` block: -->
| Option for `General` block | Description       | DataType |
|:--------:|:------:|:------|
| `ServiceXName`* | ServiceX backend name in your `servicex.yaml` file <br>  | `String` |
| `OutputDirectory` | Path to the directory for ServiceX delivered files | `String` |
| `OutputFormat`* | Output file format of ServiceX delivered data (`parquet` or `root` for `uproot` / `root` for `xaod`) | `String` |
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
| `FuncADL` | func-adl expression for a given sample |`String` |
| `LocalPath` | File path directly from local path (NO ServiceX tranformation) | `String` |

 <!-- Options exclusively for TCut syntax (CANNOT combine with the option `FuncADL`) -->

 <!-- Option for func-adl expression (CANNOT combine with the option `Fitler` and `Columns`) -->

A config file can be simplified by utilizing `Definition` block. You can define placeholders under `Definition` block, which will replace all matched placeholders in the values of `Sample` block. Note that placeholders must start with `DEF_`.

You can source each Sample using different ServiceX transformers. 
The default transformer is set by `type` of `servicex.yaml`, but `Transformer` in the `General` block overwrites if present, and `Transformer` in each `Sample` overwrites any previous transformer selection.

The [following example configuration](config_maximum.yaml) shows how to use each Options.

```yaml
General:
  ServiceXName: servicex-uc-af
  Transformer: uproot
  OutputFormat: root
  OutputDirectory: /Users/kchoi/data_for_MLstudy
  WriteOutputDict: fileset_ml_study
  IgnoreServiceXCache: False
  
Sample:  
  - Name: Signal
    RucioDID: user.kchoi:user.kchoi.signalA,
              user.kchoi:user.kchoi.signalB,
              user.kchoi:user.kchoi.signalC
    Tree: nominal
    FuncADL: DEF_ttH_nominal_query
  - Name: Background1
    XRootDFiles: DEF_ggH_input
    Tree: mini
    Filter: lep_n>2
    Columns: lep_pt, lep_eta
  - Name: Background2
    Transformer: atlasr21
    RucioDID: DEF_Zee_input
    FuncADL: DEF_Zee_query
  - Name: Background3
    LocalPath: /Users/kchoi/Work/data/background3

Definition:
  DEF_ttH_nominal_query: "Where(lambda e: e.met_met>150e3). \
              Select(lambda event: {'el_pt': event.el_pt, 'jet_e': event.jet_e, \
              'jet_pt': event.jet_pt, 'met_met': event.met_met})"
  DEF_ggH_input: "root://eospublic.cern.ch//eos/opendata/atlas/OutreachDatasets\
                  /2020-01-22/4lep/MC/mc_345060.ggH125_ZZ4lep.4lep.root"
  DEF_Zee_input: "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.\
                merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00"
  DEF_Zee_query: "SelectMany('lambda e: e.Jets(\"AntiKt4EMTopoJets\")'). \
              Where('lambda j: (j.pt() / 1000) > 30'). \
              Select('lambda j: j.pt() / 1000.0'). \
              AsROOTTTree('junk.root', 'my_tree', [\"JetPt\"])"
```


## Deliver data

```python
from servicex_databinder import DataBinder
sx_db = DataBinder('<CONFIG>.yml')
out = sx_db.deliver()
```

The function `deliver()` returns a Python nested dictionary that contains delivered files.
<!-- - for `uproot` backend and `parquet` output format: `out['<SAMPLE>']['<TREE>'] = [ List of output parquet files ]`
- for `uproot` backend and `root` output format: `out['<SAMPLE>'] = [ List of output root files ]`
- for `xAOD` backend: `out['<SAMPLE>'] = [ List of output root files ]` -->

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