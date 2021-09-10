from typing import Any, Dict, List, Optional, Union
import tcut_to_qastle as tq

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
                'gridDID': did,
                'query': query
                }
            )
        return requests_sample
        

    def _build_query(self, sample: Dict) -> str:
        query = tq.translate(
            sample['Tree'],
            sample['Columns'],
            sample['Filter']
        )
        return query