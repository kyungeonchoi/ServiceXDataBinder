from typing import Any, Dict, List
import tcut_to_qastle as tq
import qastle
import ast
import logging
from func_adl_servicex import ServiceXSourceXAOD

log = logging.getLogger(__name__)

class ServiceXRequest():
    """Prepare ServiceX requests"""
    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = config
        self._backend = self._config.get('General')['ServiceXBackendName'].lower()

    def get_requests(self) -> List:
        log.debug(f"ServiceX backend: {self._config.get('General')['ServiceXBackendName']}")
        list_requests = []
        for sample in self._config.get('Sample'):
            list_requests.append(self._build_request(sample))
        flist_requests = [request for x in list_requests for request in x] # flatten nested lists
        log.debug(f"number of total ServiceX requests in the config: {len(flist_requests)}")
        return flist_requests


    def _build_request(self, sample: Dict) -> Dict:
        requests_sample = []
        query = self._build_query(sample)
        if 'RucioDID' in sample.keys():
            dids = sample['RucioDID'].split(',')
            if 'uproot' in self._backend:
                log.debug(f"  Sample {sample['Name']} - {sample['Tree']} has {len(dids)} DID(s)")
            elif 'xaod' in self._backend:
                log.debug(f"  Sample {sample['Name']} has {len(dids)} DID(s)")
            for did in dids:
                requests_sample.append(
                    {
                    'Sample': sample['Name'],
                    'dataset': did.strip(),
                    'query': query
                    }
                )
        elif 'XRootDFiles' in sample.keys():
            xrootd_filelist = [file.strip() for file in sample['XRootDFiles'].split(",")]
            if 'uproot' in self._backend:
                log.debug(f"  Sample {sample['Name']} - {sample['Tree']} has {len(xrootd_filelist)} file(s)")
            elif 'xaod' in self._backend:
                log.debug(f"  Sample {sample['Name']} has {len(xrootd_filelist)} file(s)")
            requests_sample.append(
                {
                'Sample': sample['Name'],
                'dataset': xrootd_filelist,
                'query': query
                } 
            )
        return requests_sample
        

    def _build_query(self, sample: Dict) -> str:
        """ Option Columns for TCut syntax and Option FuncADL for func-adl syntax"""        
        if 'uproot' in self._backend:
            if 'Columns' in sample:
                if 'Filter' not in sample or sample['Filter'] == None: sample['Filter'] = ''
                try:
                    query = tq.translate(
                        sample['Tree'],
                        sample['Columns'],
                        sample['Filter']
                    )
                    return query
                except:
                    log.exception(f"Exception occured for the query of Sample {sample['Name']}")
            elif 'FuncADL' in sample:
                query = f"EventDataset('ServiceXDatasetSource', '{sample['Tree']}')." + sample['FuncADL']
                try:
                    qastle_query = qastle.python_ast_to_text_ast(qastle.insert_linq_nodes(ast.parse(query)))
                    return qastle_query
                except:
                    log.exception(f"Exception occured for the query of Sample {sample['Name']}")
        elif 'xaod' in self._backend:
            query = f"ServiceXSourceXAOD('')." + sample['FuncADL']
            try:
                o = eval(query)
                qastle_query = qastle.python_ast_to_text_ast(o._q_ast)
                return qastle_query
            except:
                log.exception(f"Exception occured for the query of Sample {sample['Name']}")