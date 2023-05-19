from typing import Any, Dict, List
import tcut_to_qastle as tq
import qastle
import ast
import logging
from func_adl_servicex import ServiceXSourceXAOD # NOQA
from servicex import ServiceXDataset # NOQA

log = logging.getLogger(__name__)


class ServiceXRequest():
    """
    Prepare ServiceX requests
    """
    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = config

    def get_requests(self) -> List:
        log.debug(f"ServiceX backend: "
                  f"{self._config.get('General')['ServiceXName']}")
        list_requests = []
        for sample in self._config.get('Sample'):
            list_requests.append(self._build_request(sample))
        # flatten nested lists
        flist_requests = [request for x in list_requests for request in x]
        log.debug("number of total ServiceX requests in the config: "
                  f"{len(flist_requests)}")
        log.debug(f"ServiceX requests in the config: {flist_requests}")
        return flist_requests

    def _build_request(self, sample: Dict) -> Dict:
        """
        Return a list containing ServiceX request(s) of the given sample
        """
        requests_sample = []

        if 'RucioDID' in sample.keys():
            dids = sample['RucioDID'].split(',')
            if sample['Type'] == "uproot":
                trees = sample['Tree'].split(',')
                log.debug(f"  Sample {sample['Name']} has "
                          f"{len(dids)} DID(s) and {len(trees)} Tree(s)")
            elif sample['Type'] == "xaod":
                trees = ['dummy']
                log.debug(f"  Sample {sample['Name']} has {len(dids)} DID(s)")

            for tree in trees:
                for did in dids:
                    requests_sample.append(
                        {
                            'Sample': sample['Name'],
                            'dataset': did.strip(),
                            'type': sample['Type'],
                            'codegen': sample['Transformer'],
                            'tree': tree.strip(),
                            'query': self._build_query(sample, tree.strip())
                        }
                    )
        elif 'XRootDFiles' in sample.keys():
            xrootd_filelist = [file.strip()
                               for file in sample['XRootDFiles'].split(",")]
            if sample['Type'] == "uproot":
                trees = sample['Tree'].split(',')
                log.debug(f"  Sample {sample['Name']} has "
                          f"{len(xrootd_filelist)} file(s) and "
                          f"{len(trees)} Tree(s)")
            elif sample['Type'] == "xaod":
                trees = ['dummy']
                log.debug(f"  Sample {sample['Name']} has "
                          f"{len(xrootd_filelist)} file(s)")
            for tree in trees:
                requests_sample.append(
                    {
                        'Sample': sample['Name'],
                        'dataset': xrootd_filelist,
                        'type': sample['Type'],
                        'codegen': sample['Transformer'],
                        'tree': tree.strip(),
                        'query': self._build_query(sample, tree)
                    }
                )
        return requests_sample

    def _build_query(self, sample: Dict, tree: str) -> str:
        """
        Get query for each sample
        Option Columns for TCut syntax
        Option FuncADL for func-adl syntax
        """
        if sample['Transformer'] == "uproot":
            if 'Columns' in sample:
                if ('Filter' not in sample) or (sample['Filter'] is None):
                    sample['Filter'] = ''
                # else:
                try:
                    query = tq.translate(
                        tree,
                        sample['Columns'],
                        sample['Filter']
                    )
                    return query
                except Exception:
                    log.exception(
                        "Exception occured for the query "
                        f"of Sample {sample['Name']}"
                        )
            elif 'FuncADL' in sample:
                query = ("EventDataset('ServiceXDatasetSource', "
                         f"'{tree}')." + sample['FuncADL'])
                try:
                    qastle_query = qastle.python_ast_to_text_ast(
                        qastle.insert_linq_nodes(ast.parse(query)))
                    return qastle_query
                except Exception:
                    log.exception(
                        "Exception occured for the query "
                        f"of Sample {sample['Name']}"
                        )
        elif sample['Transformer'] == "atlasr21":
            query = "ServiceXSourceXAOD(ServiceXDataset('', backend_name='" \
                + self._config.get('General')['ServiceXName'] + "'))." \
                + sample['FuncADL']
            try:
                o = eval(query)
                qastle_query = qastle.python_ast_to_text_ast(o._q_ast)
                return qastle_query
            except Exception:
                log.exception(
                    "Exception occured for the query "
                    f"of Sample {sample['Name']}"
                    )
