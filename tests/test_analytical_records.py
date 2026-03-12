import unittest
from datetime import date, datetime, timedelta, timezone
import os
import configparser
import pandas as pd
import pymongo
from almasru import SruClient

from almapiwrapper.inventory import IzBib

from copy_analytical_records_from_nz_to_iz import copy_analytical_rec_from_nz, is_record_in_iz_already_existing
from fetch_analytical_records_and_copy import is_accepted_record, get_ids, transform_iz_mms_id_to_nz_mms_id, get_record

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
update_data_threshold = datetime.now(timezone.utc) - timedelta(days=int(config['process']['update_delay_days']))
creation_date_threshold = datetime(int(config['process']['creation_year_threshold']), 1, 1, tzinfo=timezone.utc)
SruClient.set_base_url('https://swisscovery.slsp.ch/view/sru/41SLSP_UBS')

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


class TestCopyAnalyticalRecFromNZ(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Set up any necessary resources for the tests
        nz_mms_id = '991172821978605501'
        iz = 'UBS'
        env = 'S'
        b = IzBib(nz_mms_id, iz, env, from_nz_mms_id=True, copy_nz_rec=False)
        if not b.error:
            # If the record already exists in the IZ, delete it to ensure a clean test environment
            b.delete()

        mapping_iz_to_nz = pd.read_csv(mapping_iz_to_nz_path, index_col='iz_mms_id', dtype=str)
        mapping_iz_to_nz = mapping_iz_to_nz.drop('9972761871805504', errors='ignore')
        mapping_iz_to_nz.to_csv(mapping_iz_to_nz_path)


    def test_copy_analytical_rec_from_nz(self):
        nz_mms_id = '991172821978605501'
        iz = 'UBS'
        env = 'S'
        f990a_txt = 'bfdnoanauto'
        f998a_txt = 'no_inventory_analytical'
        self.assertFalse(is_record_in_iz_already_existing(nz_mms_id, iz, env))
        copy_analytical_rec_from_nz(nz_mms_id, iz, env, f990a_txt, f998a_txt)
        self.assertTrue(is_record_in_iz_already_existing(nz_mms_id, iz, env))
        b = IzBib(nz_mms_id, iz, env, from_nz_mms_id=True, copy_nz_rec=False)
        self.assertGreater(len(b.data.xpath(f'.//datafield[@tag="990"]/subfield[@code="a" and text()="{f990a_txt}"]')), 0)
        self.assertGreater(len(b.data.xpath(f'.//datafield[@tag="998"]/subfield[@code="a" and text()="{f998a_txt}"]')), 0)

    def test_get_ids(self):
        nz_mms_id = '991008303279705501'
        r = get_record(nz_mms_id)
        self.assertListEqual(get_ids(r),
                             ['991008303279705501', '(swissbib)212019058-41slsp_network', '(IDSLU)000135643ILU01', '(RERO)0993286-41slsp'])

    def test_transform_iz_mms_id_to_nz_mms_id(self):
        iz_mms_id = '9972761871805504'
        mapping_iz_to_nz = pd.read_csv(mapping_iz_to_nz_path, index_col='iz_mms_id', dtype=str)
        self.assertFalse('9972761871805504' in mapping_iz_to_nz.index)
        self.assertEqual(transform_iz_mms_id_to_nz_mms_id(iz_mms_id), '991171363729705501')
        mapping_iz_to_nz = pd.read_csv(mapping_iz_to_nz_path, index_col='iz_mms_id', dtype=str)
        self.assertTrue('9972761871805504' in mapping_iz_to_nz.index)

    def test_is_accepted_record(self):
        # 580$$a with "Sonder"
        r = get_record('991145640519705501')
        self.assertFalse(is_accepted_record(r))

        # 500$$a with "Sonder"
        r = get_record('991171017336505501')
        self.assertFalse(is_accepted_record(r))

        # 773$$g with "Sonder"
        r = get_record('991171867641205501')
        self.assertFalse(is_accepted_record(r))

        # Online record
        r = get_record('991171841756605501')
        self.assertFalse(is_accepted_record(r))

        # not analytical record
        r = get_record('991171018126505501')
        self.assertFalse(is_accepted_record(r))

        # Accepted record
        r = get_record('991170920778005501')
        self.assertTrue(is_accepted_record(r))

        r = get_record('991170890577205501')
        self.assertTrue(is_accepted_record(r))

        r = get_record('991170944774905501')
        self.assertTrue(is_accepted_record(r))

        r = get_record('991170898894705501')
        self.assertTrue(is_accepted_record(r))

        r = get_record('991170886279705501')
        self.assertTrue(is_accepted_record(r))



    @classmethod
    def tearDownClass(cls):
        # Clean up any resources used during the tests
        nz_mms_id = '991172821978605501'
        iz = 'UBS'
        env = 'S'
        b = IzBib(nz_mms_id, iz, env, from_nz_mms_id=True, copy_nz_rec=False)
        if not b.error:
            # If the record exists in the IZ, delete it to clean up after the test
            b.delete()
        mapping_iz_to_nz = pd.read_csv(mapping_iz_to_nz_path, index_col='iz_mms_id', dtype=str)
        mapping_iz_to_nz = mapping_iz_to_nz.drop('9972761871805504', errors='ignore')
        mapping_iz_to_nz.to_csv(mapping_iz_to_nz_path)





if __name__ == '__main__':
    unittest.main()
