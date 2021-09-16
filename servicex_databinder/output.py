from pathlib import Path
from shutil import copy, rmtree
import shutil
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
        
        def file_exist_in_out_path(out, out_path) -> bool:
            # print(f"out:\n {[Path(out_path, str(fi).split('/')[-1]) for fi in out]}")
            # print(f"local:\n {list(Path(out_path).glob('*'))}")
            a = [Path(out_path, str(fi).split('/')[-1]) for fi in out]
            b = list(Path(out_path).glob('*'))
            return set(a) <= set(b)

        """
        Compare cache queries before and after making ServiceX requests. 
        Copy parquet files for new queries.
        """
        all_files_in_requests = []
        for req, out in zip(request, output):
            out_path = f"{output_path}/{req['Sample']}/{get_tree_name(req['query'])}/"
            Path(out_path).mkdir(parents=True, exist_ok=True)
            if get_cache_query(req) in current_cache: # Matched query in cache
                if file_exist_in_out_path(out, out_path): # Already copied
                    all_files_in_requests.append([Path(out_path, str(fi).split('/')[-1]) for fi in out])
                    out_paths[req['Sample']][get_tree_name(req['query'])] = \
                        glob(f"{config['General']['OutputDirectory']}/{req['Sample']}/{get_tree_name(req['query'])}/*")
                else: # Cached but not copied
                    for src in out: copy(src, out_path)
                    all_files_in_requests.append([Path(out_path, str(fi).split('/')[-1]) for fi in out])
                    out_paths[req['Sample']][get_tree_name(req['query'])] = \
                            glob(f"{config['General']['OutputDirectory']}/{req['Sample']}/{get_tree_name(req['query'])}/*")
            else: # New or modified requests
                for src in out: copy(src, out_path)
                all_files_in_requests.append([Path(out_path, str(fi).split('/')[-1]) for fi in out])
                out_paths[req['Sample']][get_tree_name(req['query'])] = \
                        glob(f"{config['General']['OutputDirectory']}/{req['Sample']}/{get_tree_name(req['query'])}/*")

        """
        Clean up parquet files in the output path if they are not in the requests
        1. delete undefined Samples
        2. delete undefined Trees
        3. delete undefined Parquet files
        """
        local_samples = [str(sa).split('/')[-1] for sa in list(Path(output_path).glob('*'))]
        # print(f"Local samples: {local_samples}, Samples in request: {samples}")
        samples_not_in_request = list(set(local_samples) ^ set(samples))
        for sa in samples_not_in_request:
            # print(f"deleting {sa}")
            shutil.rmtree(Path(output_path, sa))

        for sample in samples:
            local_trees = []
            for tree in list(Path(output_path,sample).glob('*')):
                local_trees.append(str(tree).split('/')[-1])
            trees_in_request = list(out_paths[sample].keys())
            trees_not_in_request = list(set(local_trees) ^ set(trees_in_request))
            # print(f"{sample} - local trees: {local_trees}, trees in requests: {trees_in_request}")
            for tr in trees_not_in_request:
                # print(f"deleting {sa}")
                shutil.rmtree(Path(output_path, sample, tr))
            
        all_files_in_local = []
        for sample in samples:
            for tree in list(Path(output_path,sample).glob('*')):
                all_files_in_local.append(list(Path(tree).glob('*')))
        all_files_in_local = [f for x in all_files_in_local for f in x]
        all_files_in_requests = [f for x in all_files_in_requests for f in x]
        # print(f"Nlocal: {len(all_files_in_local)}, NReq: {len(all_files_in_requests)}")
        files_not_in_request = list(set(all_files_in_local) ^ set(all_files_in_requests))
        # print(files_not_in_request)
        for fi in files_not_in_request:
            Path.unlink(fi)


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
