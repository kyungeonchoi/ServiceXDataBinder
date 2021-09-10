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

        return list_requests

    def _build_request(self, sample: Dict) -> Dict:
        query = self._build_query(sample)
        return {
            'Sample': sample['Name'],
            'gridDID': sample['GridDID'],
            'query': query
        }

    def _build_query(self, sample: Dict) -> str:
        query = tq.translate(
            sample['Tree'],
            sample['Columns'],
            sample['Filter']
        )
        return query