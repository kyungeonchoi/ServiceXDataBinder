from pathlib import Path
from shutil import copy
from typing import Dict, Any, List
from glob import glob
import re
import yaml
from servicex import ServiceXDataset


def _output_handler(config:Dict[str, Any], request, output, current_cache:List) -> Dict[str,List]:
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

    """ Create output directory """
    output_path = ''
    if 'OutputDirectory' in config['General'].keys():
        Path(f"{config['General']['OutputDirectory']}").mkdir(parents=True, exist_ok=True)
        output_path = config['General']['OutputDirectory']
    else:
        Path('ServiceXData').mkdir(parents=True, exist_ok=True)
        output_path = 'ServiceXData'
    
    
    # Prepare output path dictionary
    out_paths = {}
    samples = [sample['Name'] for sample in config['Sample']]
    for sample in samples:
            out_paths[sample] = {}


    # Uproot + parquet
    if config['General']['OutputFormat'] == "parquet" and config['General']['ServiceXBackendName'].lower() == "uproot":

        def get_tree_name(query:str) -> str:
            o = re.search(r"ServiceXDatasetSource' '\w+'", query)
            return o.group(0).split(" ")[1].strip("\"").replace("'","")

        def get_cache_query(request:Dict) -> bool:
            cache_path = ServiceXDataset("",backend_name="uproot")._cache._path
            query_cache_status = Path.joinpath(cache_path, "query_cache_status")

            for query_cache in list(query_cache_status.glob('*')):
                q = open(query_cache).read()
                if request['query'] in q and request['gridDID'].strip() in q:
                    return query_cache

        # Compare cache queries before and after making ServiceX requests. And then copy only new queries.
        for req, out in zip(request, output):
            # print(f"Sample: {req['Sample']}, DID: {req['gridDID']}")
            # print(f"get_cache_query: {get_cache_query(req)}")
            if get_cache_query(req) in current_cache:
                # print(f"Request is in cache")
                out_paths[req['Sample']][get_tree_name(req['query'])] = \
                    glob(f"{config['General']['OutputDirectory']}/{req['Sample']}/{get_tree_name(req['query'])}/*")
            else:
                # print(f"Request is NOT in cache")
                out_path = f"{output_path}/{req['Sample']}/{get_tree_name(req['query'])}/"
                Path(out_path).mkdir(parents=True, exist_ok=True)
                for src in out: copy(src, out_path)
            # print("\n")

        # TODO: newly added DID to a Sample can be handled by above loop, but nothing done if a DID is removed from Sample. 
    
    if 'WriteOutputDict' in config['General'].keys():
        with open(f"{config['General']['WriteOutputDict']}.yml", 'w') as outfile:
            yaml.dump(out_paths, outfile, default_flow_style=False)

    print(f'4/4 Done')
    return out_paths


        # def get_old_cache_filelist(request:Dict) -> bool:
        #     cache_path = ServiceXDataset("",backend_name="uproot")._cache._path
        #     query_cache_status = Path.joinpath(cache_path, "query_cache_status")

        #     for query_cache in list(query_cache_status.glob('*')):
        #         q = open(query_cache).read()
        #         # if request['gridDID'].strip() in query: # and request['query'] in query:
        #         # if request['query'] in q:
        #         if request['query'] in q and request['gridDID'].strip() in q:
        #             print("Identical")
        #             print(request['Sample'])
        #             print(request['gridDID'])
        #             print(request['query'])
        #             print(q)
        #             # file_path = Path(str(query_cache).replace('query_cache_status','data'))
        #             # return [str(p).split('/')[-1] for p in file_path.iterdir() if p.is_file()]
        #             return True
        #         else:
        #             # print("Different")
        #             # print(request['Sample'])
        #             # print(request['gridDID'])
        #             # print(request['query'])
        #             # print(q)
        #             pass
        #     return False
