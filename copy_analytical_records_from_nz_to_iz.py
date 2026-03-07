# Import analytical records from the NZ to the defined IZ

# It adds local extensions too
# Usage: python copy_analytical_records_from_nz_to_iz.py <path_to_csv_file_with_mms_ids>

# Import libraries
from almapiwrapper.inventory import IzBib, NzBib
from almapiwrapper.configlog import config_log
from lxml import etree
from typing import Optional
import pandas as pd
import logging
import sys


def copy_analytical_rec_from_nz(mms_id: str, iz: str, env='P', f990a_txt: Optional[str] = None,
                                f998a_txt: Optional[str] = None) -> None:
    """
    Copy analytical record from the NZ to the IZ

    Parameters
    ----------
    mms_id: string: with NZ MMS ID of the record
    iz: string: string: code of the institution where the record need to be copied
    env: string: environment where the record need to be copied (default: 'P')
    f990a_txt: string: content of the field 990$$a
    f998a_txt: string: content of the field 998$$a
    """
    # Create the copy of the record
    izbib = IzBib(mms_id, zone=iz, env=env, from_nz_mms_id=True, copy_nz_rec=True)
    iz_mms_id = izbib.mms_id

    # Fetch a fresh copy of the record
    izbib = IzBib(iz_mms_id, zone=iz, env=env)

    # Add the two local extensions
    record = izbib.data.find('record')

    if f990a_txt is not None and len(
            izbib.data.xpath(f'.//datafield[@tag="990"]/subfield[@code="a" and text()="{f990a_txt}"]')) == 0:
        f990 = etree.SubElement(record, 'datafield', tag='990', ind1=' ', ind2=' ')
        etree.SubElement(f990, 'subfield', code='a').text = f990a_txt
        etree.SubElement(f990, 'subfield', code='9').text = 'LOCAL'

    if f998a_txt is not None and len(
            izbib.data.xpath(f'.//datafield[@tag="998"]/subfield[@code="a" and text()="{f998a_txt}"]')) == 0:
        f998 = etree.SubElement(record, 'datafield', tag='998', ind1=' ', ind2=' ')
        etree.SubElement(f998, 'subfield', code='a').text = f998a_txt
        etree.SubElement(f998, 'subfield', code='9').text = 'LOCAL'

    izbib.sort_fields()
    izbib.update()


if __name__ == '__main__':

    from pathlib import Path
    from dotenv import load_dotenv

    config_log(Path(sys.argv[1]).stem)

    CURRENT_DIR = Path(__file__).resolve().parent

    # .env in the parent folder
    ENV_PATH = CURRENT_DIR.parent / ".env"

    load_dotenv(ENV_PATH)

    # nz_mms_ids = pd.read_csv('data/records_to_copy_from_NZ_to_IZ.csv', dtype=str)['mms_id'].values
    nz_mms_ids = pd.read_csv(sys.argv[1], dtype=str)['mms_id'].values

    f990a_txt = 'bfdnoanauto'
    f998a_txt = 'no_inventory_analytical'
    iz = 'BCUFR'
    env = 'P'

    # Iterate all MMS IDs
    for i, nz_mms_id in enumerate(nz_mms_ids, start=1):
        logging.info(f'Processing record {i} / {len(nz_mms_ids)}: {nz_mms_id}')
        copy_analytical_rec_from_nz(nz_mms_id, iz, env, f990a_txt, f998a_txt)
