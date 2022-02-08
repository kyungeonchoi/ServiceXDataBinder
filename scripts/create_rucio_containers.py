import yaml
import argparse
import logging

logging.basicConfig()
logging.root.setLevel(logging.INFO)

logger = logging.getLogger("-")

try:
    from rucio.client.didclient import DIDClient
    from rucio.client.accountclient import AccountClient

    did_client = DIDClient()
    account_client = AccountClient()
except:
    raise ImportError("Please setup rucio environment!")

def existing_did(scope, name):
    try:
        did_client.get_did(scope, name)
        return True
    except:
        return False


def create_new_container(user_name, container_name, sample, version, dryrun):
    scope = 'user.' + user_name
    name = scope + '.' + container_name + '.' + sample + '.' + version
    did = scope + ':' + name

    if not dryrun:
        if existing_did(scope, name):
            logger.info("Existing DID! - skip Sample %s", sample)
            return None
        else:
            logger.info("Creating Rucio container: %s", did)
            try:
                did_client.add_container(scope, name)
            except:
                raise ValueError('Failed to create a container: %s', did)
    else:
        logger.info("Dry run!")
        pass
    return did

def add_datasets(did, sample, sample_rucio_dict, dryrun):
    scope = did.split(':')[0]
    name = did.split(':')[1]    
    dids_to_add = [{'scope': did_i.split(':')[0], 'name': did_i.split(':')[1]} for did_i in sample_rucio_dict[sample].split(" ")]

    logger.info("Adding %s datasets to %s", len(dids_to_add), did)
    if not dryrun:
        try:
            did_client.add_containers_to_container(scope, name, dids_to_add)
        except:
            raise ValueError('Failed to add dataset to %s', did)
    else:
        logger.info("Dry run!")
        pass

def close_datasets(did, dryrun):
    scope = did.split(':')[0]
    name = did.split(':')[1]
    logger.info("Closing container: %s", did)
    if not dryrun:
        try:
            did_client.close(scope,name)
        except:
            raise ValueError('Failed to close container: %s', did)
    else:
        logger.info("Dry run!")
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create Rucio containers from multiple DIDs')
    parser.add_argument('infile', type=str, help='yaml file contains Rucio DIDs for each Sample')
    parser.add_argument('container_name', type=str, help='e.g. user.kchoi:user.kchoi.<container-name>.Sample.v1')
    parser.add_argument('version', type=str, help='e.g. user.kchoi:user.kchoi.fcnc_ana.Sample.<version>')
    parser.add_argument('--dry-run', type=bool, help='Run without creating new Rucio container')

    args = parser.parse_args()

    logger.info("Loading input file")
    sample_rucio_dict = yaml.safe_load(open(args.infile))
    
    samples = sample_rucio_dict.keys()
    logger.info("Samples in the file: %s", samples)

    whoami = account_client.whoami()
    user_name = whoami['account']

    logger.info("")
    for sample in samples:        
        did = create_new_container(user_name, args.container_name, sample, args.version, args.dry_run)
        if did:
            add_datasets(did, sample, sample_rucio_dict, args.dry_run)
            close_datasets(did, args.dry_run)
        logger.info("")
