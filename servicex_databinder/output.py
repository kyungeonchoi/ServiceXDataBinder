from pathlib import Path
from shutil import copy
from typing import Dict, Any



def _output_handler(config:Dict[str, Any], request, output) -> None:
    """ 
    Manage ServiceX delivered outputs 
    uproot + parquet: create subdirectory for each sample and copy parquet files
    uproot + root: create one root file per sample
    """
    print("3/4 Post-processing..")

    if len(request) == len(output):
        pass
    else:
        raise ValueError('Something went wrong.. '
                         'Number of ServiceX requests and outputs do not agree.' 
                         'Check transformation status at dashboard')

    # Create output directory
    output_path = ''
    if 'OutputDirectory' in config['General'].keys():
        Path(f"{config['General']['OutputDirectory']}").mkdir(parents=True, exist_ok=True)
        output_path = config['General']['OutputDirectory']
    else:
        Path('ServiceXData').mkdir(parents=True, exist_ok=True)
        output_path = 'ServiceXData'
    
    # List of Samples
    samples = [sample['Name'] for sample in config['Sample']]

    # Uproot + parquet
    if config['General']['OutputFormat'] == "parquet" and config['General']['ServiceXBackendName'] == "uproot":
        for sample in samples:
            Path(f"{output_path}/{sample}").mkdir(parents=False, exist_ok=True)
            for req, out in zip(request, output):
                if req['Sample'] == sample:
                    for src in out: copy(src, f"{output_path}/{sample}")
                    # print(f"{sample} - {out}")

    print(f'4/4 Done')
        