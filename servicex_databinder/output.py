from pathlib import Path
from shutil import ExecError, copy, rmtree
from typing import Dict, Any, List
from glob import glob
import re
import yaml
import logging
from multiprocessing import Pool, cpu_count
from functools import partial
from servicex import ServiceXDataset
import awkward as ak
import uproot
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

def get_tree_name(query:str) -> str:
    o = re.search(r"ServiceXDatasetSource' '\w+'", query)
    return o.group(0).split(" ")[1].strip("\"").replace("'","")

def convert_parquet_to_root(filelist, zip_common_vector_columns = False):
    def get_n(flist, n):
        return flist[n]

    log.debug(f"Zip vector columns for root output: {zip_common_vector_columns}")

    for file in range(len(filelist[0])):
        same_file_list = []
        for tree in sorted(filelist):
            same_file_list.append(get_n(tree, file))

        if pq.read_table(same_file_list[0]).num_rows == 0:
            log.debug(f"empty parquet file: {same_file_list[0]}")
            pass
        else:
            outfile = uproot.recreate(same_file_list[0].rstrip('parquet').rstrip('.').replace(same_file_list[0].split('/')[-2] + '/', ''))
            for infile in same_file_list:
                tree_dict = {}
                ak_arr = ak.from_parquet(infile)
                if zip_common_vector_columns:
                    all_fields = ak_arr.fields
                    vec_fields = [fi for fi in ak_arr.fields if ak_arr[fi].ndim == 2]
                    vec_prefix = [item.split('_')[0] for item in vec_fields]
                    can_vec = [item for item in set(vec_prefix) if vec_prefix.count(item) > 1]
                    for pfix in can_vec:
                        dict_pfix = {}
                        for item in vec_fields:
                            if item.startswith(pfix):
                                dict_pfix[item.split('_')[1]] = ak_arr[item]
                        zipped_dict = {}
                        zipped_dict[pfix] = ak.zip(dict_pfix)
                        tree_dict.update(zipped_dict)
                    for zip_item in can_vec:
                        for item in sorted(all_fields):
                            if item.startswith(zip_item+'_'):
                                all_fields.pop(all_fields.index(item))
                    for field in all_fields:
                        tree_dict[field] = ak_arr[field]
                    outfile[infile.split('/')[-2]] = tree_dict
                else:
                    for field in ak_arr.fields:
                        tree_dict[field] = ak_arr[field]
                    outfile[infile.split('/')[-2]] = tree_dict

            outfile.close()

def _parquet_to_root(config:Dict[str, Any], out_paths):
    
    log.debug("Converting parquet to ROOT")

    list_sample_tree = []
    sample_list = list(out_paths.keys())
    for sample in sample_list:
        tree_list = [out_paths[sample][tree] for tree in out_paths[sample].keys()]
        list_sample_tree.append(tree_list)

    if 'ZipROOTColumns' in config['General']:
        zip_common_vector_columns = config['General']['ZipROOTColumns']
    else:
        zip_common_vector_columns = False

    convert_parquet_to_root_zip = partial(convert_parquet_to_root, zip_common_vector_columns=zip_common_vector_columns)
    nproc = min(len(list_sample_tree), int(cpu_count()/2))
    with Pool(processes=nproc) as pool:
        pool.map(convert_parquet_to_root_zip, list_sample_tree)

def _output_handler(config:Dict[str, Any], request, output, cache_before_requests:List) -> Dict[str,List]:
    """
    Manage ServiceX delivered outputs 
    uproot + parquet: create subdirectory for each sample and copy parquet files
    uproot + root: create one root file per sample
    """
    log.info("post-processing..")

    if len(request) == len(output):
        pass
    else:
        raise ValueError('Something went wrong.. '
                         'Number of ServiceX requests and outputs do not agree.' 
                         'Check transformation status at dashboard')

    """
    Create output directory
    """
    output_path = ''
    if 'OutputDirectory' in config['General'].keys():
        Path(f"{config['General']['OutputDirectory']}").mkdir(parents=True, exist_ok=True)
        output_path = config['General']['OutputDirectory']
    else:
        Path('ServiceXData').mkdir(parents=True, exist_ok=True)
        output_path = 'ServiceXData'
    log.debug(f"output directory is {output_path}")
    
    """
    Prepare output path dictionary
    """
    out_paths = {}    
    samples = []
    [samples.append(sample['Name']) for sample in config['Sample'] if sample['Name'] not in samples]
    for sample in samples:
        out_paths[sample] = {}
    if "uproot" in config['General']['ServiceXBackendName'].lower():
        for sample in config['Sample']:
            for tree in sample['Tree'].split(','):
                out_paths[sample['Name']][tree.strip()] = {}
    
    """
    Utils
    """
    def get_cache_query(request:Dict) -> bool:
        cache_path = ServiceXDataset("",backend_name=config['General']['ServiceXBackendName'])._cache._path
        query_cache_status = Path.joinpath(cache_path, "query_cache_status")

        for query_cache in list(query_cache_status.glob('*')):
            q = open(query_cache).read()
            if type(request['dataset']) == str:
                if request['query'] in q and request['dataset'].strip() in q and yaml.safe_load(q)['status'] == 'Complete':
                    return query_cache
            elif type(request['dataset']) == list:
                if yaml.safe_load(q)['status'] == 'Complete':
                    file_list = Path(str(query_cache).replace('query_cache_status','file_list_cache'))
                    f = open(file_list).read()
                    all_files_exist = True
                    for file in request['dataset']:
                        if file.replace('/',':') not in f:
                            all_files_exist = False
                    if request['query'] in q and all_files_exist == True:
                        return query_cache
        return False

    def file_exist_in_out_path(out, out_path) -> bool:
        a = [Path(out_path, str(fi).split('/')[-1]) for fi in out]
        b = list(Path(out_path).glob('*'))
        return set(a) <= set(b)
    

    """ 
    Uproot + parquet/ROOT
    """
    if "uproot" in config['General']['ServiceXBackendName'].lower():
        """
        Compare cache queries before and after making ServiceX requests. 
        Copy parquet files for new queries.
        """
        all_files_in_requests = []
        for req, out in zip(request, output):
            out_path = f"{output_path}/{req['Sample']}/{get_tree_name(req['query'])}/"
            Path(out_path).mkdir(parents=True, exist_ok=True)
            if get_cache_query(req) in cache_before_requests: # Matched query in cache
                if file_exist_in_out_path(out, out_path): # Already copied
                    all_files_in_requests.append([Path(out_path, str(fi).split('/')[-1]) for fi in out])
                else: # Cached but not copied
                    for src in out: copy(src, out_path)
                    all_files_in_requests.append([Path(out_path, str(fi).split('/')[-1]) for fi in out])
            else: # New or modified requests
                for src in out: copy(src, out_path)
                all_files_in_requests.append([Path(out_path, str(fi).split('/')[-1]) for fi in out])

        """
        Clean up parquet files in the output path if they are not in the requests
        1. delete undefined Samples
        2. delete root files if exist
        3. delete Trees not in request
        4. delete Parquet files not in request
        """
        local_samples = [str(sa).split('/')[-1] for sa in list(Path(output_path).glob('*')) if sa.is_dir()]
        log.debug("synchronizing output path with the config")
        log.debug(f"  Samples in output directory: {local_samples}, Samples in request: {samples}")
        samples_not_in_request = list(set(local_samples) ^ set(samples))
        for sa in samples_not_in_request:
            log.debug(f"    deleting Sample {sa}")
            rmtree(Path(output_path, sa))

        for sample in samples:
            for root_file in list(Path(output_path,sample).glob('*root')):
                Path.unlink(root_file)
    
        for sample in samples:
            local_trees = []
            for tree in list(Path(output_path,sample).glob('*')):
                local_trees.append(str(tree).split('/')[-1])
            trees_in_request = list(out_paths[sample].keys())
            trees_not_in_request = list(set(local_trees) ^ set(trees_in_request))
            log.debug(f"  {sample} - local trees: {local_trees}, trees in requests: {trees_in_request}")
            for tr in trees_not_in_request:
                log.debug(f"    deleting Tree {tr}")
                rmtree(Path(output_path, sample, tr))
            
        all_files_in_local = []
        for sample in samples:
            for tree in list(Path(output_path,sample).glob('*')):
                all_files_in_local.append(list(Path(tree).glob('*')))
        all_files_in_local = [f for x in all_files_in_local for f in x]
        all_files_in_requests = [f for x in all_files_in_requests for f in x]
        log.debug(f"  #files in local: {len(all_files_in_local)}, #files in requests: {len(all_files_in_requests)}")
        files_not_in_request = list(set(all_files_in_local) ^ set(all_files_in_requests))
        if files_not_in_request: 
            log.debug(f"    deleting {len(files_not_in_request)} files")
            for fi in files_not_in_request:
                Path.unlink(fi)

        """
        Delete parquet file with zero entry
        """
        for file in all_files_in_requests:
            if pq.read_table(file).num_rows == 0:
                Path.unlink(file)
        
        """
        Dictionary of output file paths for parquet
        """
        for sample in samples:
            for tree in list(Path(output_path,sample).glob('*')):
                tree = tree.name
                out_paths[sample][tree] = \
                    glob(f"{str(Path(config['General']['OutputDirectory'], sample, tree).resolve())}/*.parquet")
        
        """
        Convert to ROOT ntuple if specified
        """
        if config['General']['OutputFormat'].lower() == "root":
            _parquet_to_root(config, out_paths)
            
            out_paths.clear() # clear paths for parquet files

            for sample in samples:
                for sa in list(Path(output_path,sample).glob('*')):
                    if sa.is_dir():
                        rmtree(sa)
                out_paths[sample] = glob(f"{str(Path(config['General']['OutputDirectory'], sample).resolve())}/*.root*")



    """ 
    xAOD + ROOT
    """
    if config['General']['OutputFormat'].lower() == "root" and "xaod" in config['General']['ServiceXBackendName'].lower():
        """
        Compare cache queries before and after making ServiceX requests. 
        Copy parquet files for new queries.
        """
        all_files_in_requests = []
        for req, out in zip(request, output):
            out_path = f"{output_path}/{req['Sample']}/"
            Path(out_path).mkdir(parents=True, exist_ok=True)
            if get_cache_query(req) in cache_before_requests: # Matched query in cache
                if file_exist_in_out_path(out, out_path): # Already copied
                    all_files_in_requests.append([Path(out_path, str(fi).split('/')[-1]) for fi in out])
                    out_paths[req['Sample']] = glob(f"{str(Path(config['General']['OutputDirectory'], req['Sample']).resolve())}/*")
                else: # Cached but not copied
                    for src in out: copy(src, out_path)
                    all_files_in_requests.append([Path(out_path, str(fi).split('/')[-1]) for fi in out])
                    out_paths[req['Sample']] = glob(f"{str(Path(config['General']['OutputDirectory'], req['Sample']).resolve())}/*")
            else: # New or modified requests
                for src in out: copy(src, out_path)
                all_files_in_requests.append([Path(out_path, str(fi).split('/')[-1]) for fi in out])
                out_paths[req['Sample']] = glob(f"{str(Path(config['General']['OutputDirectory'], req['Sample']).resolve())}/*")

        """
        Clean up parquet files in the output path if they are not in the requests
        1. delete undefined Samples        
        2. delete undefined root files
        """
        local_samples = [str(sa).split('/')[-1] for sa in list(Path(output_path).glob('*')) if sa.is_dir()]
        log.debug("synchronizing output path with the config")
        log.debug(f"  Samples in output directory: {local_samples}, Samples in request: {samples}")
        samples_not_in_request = list(set(local_samples) ^ set(samples))
        for sa in samples_not_in_request:
            log.debug(f"    deleting Sample {sa}")
            rmtree(Path(output_path, sa))
            
        all_files_in_local = []
        for sample in samples:
            all_files_in_local.append(list(Path(output_path, sample).glob('*')))
        all_files_in_local = [f for x in all_files_in_local for f in x]
        all_files_in_requests = [f for x in all_files_in_requests for f in x]
        log.debug(f"  #files in local: {len(all_files_in_local)}, #files in requests: {len(all_files_in_requests)}")
        files_not_in_request = list(set(all_files_in_local) ^ set(all_files_in_requests))
        if files_not_in_request: 
            log.debug(f"    deleting {len(files_not_in_request)} files")
            for fi in files_not_in_request:
                Path.unlink(fi)


    """ Write yaml of output dict """
    if 'WriteOutputDict' in config['General'].keys():
        with open(f"{output_path}/{config['General']['WriteOutputDict']}.yml", 'w') as outfile:
            log.debug(f"write a yaml file containg output paths: {outfile.name}")
            yaml.dump(out_paths, outfile, default_flow_style=False)
    else:
        for yl in list(Path(output_path).glob("*yml")):
            Path.unlink(yl)


    log.info("done.")
    return out_paths
