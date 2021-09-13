from typing import Any, Dict, List, Optional, Union
import tcut_to_qastle as tq
import qastle
import ast


class ServiceXRequest():
    """Prepare ServiceX requests"""
    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = config


    def get_requests(self) -> List:
        list_requests = []
        for sample in self._config.get('Sample'):
            list_requests.append(self._build_request(sample))
        return [request for x in list_requests for request in x] # flatten nested lists


    def _build_request(self, sample: Dict) -> Dict:
        requests_sample = []
        query = self._build_query(sample)
        
        for did in sample['GridDID'].split(','):
            requests_sample.append(
                {
                'Sample': sample['Name'],
                'gridDID': did.strip(),
                'query': query
                }
            )
        return requests_sample
        

    def _build_query(self, sample: Dict) -> str:
        """ Option Columns for TCut syntax and Option FuncADL for func-adl syntax"""
        if 'Columns' in sample:
            if 'Filter' not in sample or sample['Filter'] == None: sample['Filter'] = ''
            query = tq.translate(
                sample['Tree'],
                sample['Columns'],
                sample['Filter']
            )
            return query
        elif 'FuncADL' in sample:
            query = f"EventDataset('ServiceXDatasetSource', '{sample['Tree']}')." + sample['FuncADL']
            return qastle.python_ast_to_text_ast(qastle.insert_linq_nodes(ast.parse(query)))
