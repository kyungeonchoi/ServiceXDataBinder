from pathlib import Path
from shutil import copy
from typing import Dict, Any, List
from glob import glob
import re



def _output_handler(config:Dict[str, Any], request, output) -> Dict[str,List]:
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

    out_paths = {}
    # Uproot + parquet
    if config['General']['OutputFormat'] == "parquet" and config['General']['ServiceXBackendName'].lower() == "uproot":

        def get_tree_name(query:str) -> str:
            o = re.search(r"ServiceXDatasetSource' '\w+'", query)
            return o.group(0).split(" ")[1].strip("\"").replace("'","")

        for sample in samples:
            for req, out in zip(request, output):                
                if req['Sample'] == sample:
                    out_path = f"{output_path}/{sample}/{get_tree_name(req['query'])}/"
                    # print(f"out path: {out_path}")
                    if Path(out_path).exists():
                        # print(f"delete existing files..")
                        for file in Path(out_path).glob('*'):
                            file.unlink()
                    else:
                        # print(f"directory doesn't exist..")
                        Path(out_path).mkdir(parents=True, exist_ok=True)
                    for src in out: copy(src, out_path)
            out_paths[sample] = {}
            out_paths[sample][get_tree_name(req['query'])] = glob(f"{config['General']['OutputDirectory']}/{sample}/{get_tree_name(req['query'])}/*")
    
    print(f'4/4 Done')
    return out_paths
    
        