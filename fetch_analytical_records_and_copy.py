# Fetch analytical records related to a set of parent MMS IDs

# Import libraries
from almapiwrapper.config import LogicalSet
from almapiwrapper.configlog import config_log
from almasru.client import SruClient, IzSruRecord
import pandas as pd
import os
import pymongo
from copy_analytical_records_from_nz_to_iz import copy_analytical_rec_from_nz
import logging
import re
import configparser
from typing import Dict, List, Set, Any, Optional
from datetime import date, datetime, timedelta, timezone

# Config logs
config_log()
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Load configuration from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Environment parameters
zone = config['environment']['zone']
env = config['environment']['environment']

# Process parameters
f990a_txt = config['process']['f990a_txt']
f998a_txt = config['process']['f998a_txt']
mapping_iz_to_nz_path: str = config['process']['mapping_iz_to_nz']
existing_records_path = config['process']['existing_records']
set_of_parents_id = config['process']['set_of_parents_id']
update_delay_days = int(config['process']['update_delay_days'])
SruClient.set_base_url(config['process']['sru_url'])

# Report parameters
report_db = config['report']['mongo_db_report']
report_col = config['report']['mongo_col_report']

# MongoDB parameters
mongo_db_name = config['mongo_db']['mongo_db']
mongo_col_name = config['mongo_db']['mongo_col']

# Initialize MongoDB client and collections
mongo_uri = os.getenv('MONGODB_URI')
mongo_client = pymongo.MongoClient(mongo_uri)
mongo_col = mongo_client[mongo_db_name][mongo_col_name]
mongo_report_col = mongo_client[report_db][report_col]

# Load mapping of IZ MMS IDs to NZ MMS IDs
mapping_iz_to_nz = pd.read_csv(mapping_iz_to_nz_path, index_col='iz_mms_id', dtype=str)

# Load existing analytical records MMS IDs to avoid duplicates
with open(existing_records_path, 'r') as f_existing_records:
    next(f_existing_records)  # Ignore header
    bcufr_analytical_records_mms_ids = set(line.strip() for line in f_existing_records if line.strip())


def is_slsp_035(rec_id: str) -> bool:
    """
    Return True if a 035 $a identifier belongs to SLSP, based on known prefixes.

    An identifier is considered SLSP if it starts with one of the prefixes in the
    form "(PREFIX)...", e.g., "(RERO)...", "(swissbib)...".

    Args:
        rec_id: The value of a 035$a (e.g., "(RERO)1575774-41slsp").

    Returns:
        True if the identifier matches the SLSP scheme; False otherwise.
    """
    prefixes = '|'.join([
        'RERO', 'IDSBB', 'IDSLU', 'IDSSG', 'NEBIS', 'SBT', 'ALEX', 'ABN',
        'swissbib', 'HAN', 'CMG-HEM'
    ])
    return bool(re.match(r'^\((?:{})\).+'.format(prefixes), rec_id))


def get_f035as(record: Dict[str, Any]) -> List[str]:
    """
    Extract all 035 $a values from a MARC record.

    Expected structure (based on your sample):
        record["marc"]["035"] = [
            {"sub": [{"a": "..."} , {"n9": "..."} , ...]},
            ...
        ]

    Args:
        record: The full MongoDB document (containing a "marc" key with datafields).

    Returns:
        A list of all values found for 035 $a.
    """
    recids: List[str] = []
    if '035' in record['marc']:
        for df in record['marc']['035']:
            for sf in df['sub']:
                if 'a' in sf:
                    recids.append(sf['a'])
    return recids


def get_record(mms_id: str) -> Dict[str, Any]:
    """
    Fetch a single record by its MMS ID.

    Args:
        mms_id: The MMS ID of the record.

    Returns:
        The MongoDB document if found.
    """
    return mongo_col.find_one({'mms_id': mms_id})


def get_ids(record: Dict[str, Any]) -> List[str]:
    """
    Build the list of identifiers used to find analytical records:
    - includes the root record's `mms_id`
    - appends any 035 $a that are SLSP identifiers (via `is_slsp_035`)

    Args:
        record: The root record.

    Returns:
        The list of identifiers (root mms_id + SLSP 035 $a).
    """
    recids: List[str] = [record['mms_id']]
    recids += [f035a for f035a in get_f035as(record) if is_slsp_035(f035a)]
    return recids


def is_accepted_record(doc: Dict[str, Any]) -> bool:
    """
    Apply acceptance criteria to a candidate (analytical) record.

    Rules:
    - Reject if the 8th character of the leader (index 7) is not 'a'.
    - Reject if any 773 $g indicates a special issue (spécial/spéciaux/sonder),
      matched case-insensitively and tolerant of the 'é' accent.

    Args:
        doc: The candidate MARC document.

    Returns:
        True if the document is accepted; False if it should be excluded.
    """
    if doc['mms_id'] in bcufr_analytical_records_mms_ids:
        logging.warning(f'{doc["mms_id"]}: already identified as analytical record in bcufr_analytical_records_mms_ids')
        return False

    if doc['access'] != 'P':
        logging.warning(f'{doc["mms_id"]}: not a print resource')
        return False

    if doc['marc']['leader'][7] != 'a':
        logging.warning(f'{doc["mms_id"]}: leader7 is not a: {doc["marc"]["leader"]}')
        return False

    g_value_re = re.compile(r"sp[ée]cia(?:l|ux)|sonder", re.IGNORECASE)

    for df in doc['marc'].get('773', []):
        for sf in df['sub']:
            sfg = sf.get('g')
            if sfg and bool(g_value_re.search(sfg)):
                logging.warning(f'{doc["mms_id"]}: 773$$g is a special issue: {sfg}')
                return False

    return True


def get_mms_ids_of_analytical_records(parent_id: str) -> Set[Any]:
    """
    Fetch analytical child MMS IDs for a given parent MMS ID.

    Args:
        parent_id: string: MMS ID of the parent record
    Returns:
        Set of analytical child MMS IDs.
    """
    record = get_record(parent_id)
    if record is None:
        logging.warning(f'{parent_id}: record not found in MongoDB')
        return set()

    recids = get_ids(record)

    if recids:
        date_limite = datetime.now(timezone.utc) - timedelta(days=update_delay_days)
        analytical_mms_ids = []
        for doc in mongo_col.find(
                {
                    "marc.773.sub.w": {"$in": recids},
                    "u_date": {"$gte": date_limite}
                },
                {'mms_id': 1, 'access': 1, 'marc.leader': 1, '_id': 0, 'marc.773.sub.g': 1, 'u_date': 1}
            ):
            if is_accepted_record(doc):
                analytical_mms_ids.append(doc['mms_id'])
            else:
                statistics['SKIP'] += 1

        return set(analytical_mms_ids)
    return set()


def get_parent_records_from_logical_set(logical_set_id: str, iz: str) -> Set[str]:
    """
    Fetch parent MMS IDs from a logical set.

    Args:
        logical_set_id: string: ID of the logical set to fetch parent MMS IDs from
        iz: string: code of the institution where the logical set is defined

    Returns:
        List of parent MMS IDs.
    """
    return set(transform_iz_mms_id_to_nz_mms_id(member.mms_id) for member in
               LogicalSet(logical_set_id, iz, env).get_members())


def transform_iz_mms_id_to_nz_mms_id(iz_mms_id: str) -> Optional[str]:
    """
    Transform an IZ MMS ID to the corresponding NZ MMS ID.

    Args:
        iz_mms_id: string: MMS ID in the IZ

    Returns:
        Corresponding NZ MMS ID.
    """
    temp_nz_mms_id = mapping_iz_to_nz.at[iz_mms_id, 'nz_mms_id'] if iz_mms_id in mapping_iz_to_nz.index else None

    if temp_nz_mms_id is None:
        temp_nz_mms_id = IzSruRecord(iz_mms_id).get_nz_mms_id()
        if temp_nz_mms_id is not None:
            mapping_iz_to_nz.loc[iz_mms_id] = temp_nz_mms_id
            mapping_iz_to_nz.to_csv(mapping_iz_to_nz_path)
    return temp_nz_mms_id


def append_id_to_csv(file_path: str, new_id: str):
    """
    Add a new ID to a CSV file, ensuring that it is added on a new line
    and that the file is created if it does not exist.
    """
    with open(file_path, 'a', encoding='utf-8') as f:
        f.write('\n' + new_id)


def write_report():
    # Add report to MongoDB
    statistics['DATE'] = date.today().isoformat()
    statistics['TIMESTAMP'] = datetime.now()
    mongo_report_col.insert_one(statistics)


if __name__ == '__main__':
    statistics = {
        'SUCCESS': 0,
        'FAILED': 0,
        'NB_PROCESSED': 0,
        'SKIP': 0,
        'NB_PARENT_RECORDS': 0,
        'DATE': date.today().isoformat(),
        'TIMESTAMP': datetime.now()
    }

    parent_mms_ids = get_parent_records_from_logical_set(set_of_parents_id, 'BCUFR')
    statistics['NB_PARENT_RECORDS'] = len(parent_mms_ids)
    for i, parent_mms_id in enumerate(parent_mms_ids, start=1):

        statistics['NB_PROCESSED'] += 1
        logging.info(f'Processing record {i} / {statistics["NB_PARENT_RECORDS"]}: {parent_mms_id}')
        try:
            nz_mms_id_to_copy = get_mms_ids_of_analytical_records(parent_mms_id)
        except Exception as e:
            logging.error(f'{parent_mms_id}: Error while fetching analytical records MMS IDs: {e}')
            statistics['FAILED'] += 1
            continue

        for nz_mms_id in nz_mms_id_to_copy:
            bcufr_analytical_records_mms_ids.add(nz_mms_id)
            try:
                # copy_analytical_rec_from_nz(nz_mms_id, zone=zone, env=env, f990a_txt=f990a_txt, f998a_txt=f998a_txt)
                append_id_to_csv(existing_records_path, nz_mms_id)
                statistics['SUCCESS'] += 1

            except Exception as e:
                logging.error(f'{parent_mms_id} - {nz_mms_id}: Error while copying record from NZ to IZ: {e}')
                statistics['FAILED'] += 1

    write_report()
    mongo_client.close()
