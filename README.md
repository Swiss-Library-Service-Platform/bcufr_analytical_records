# BCUFR Analytical Records

This repository contains a script to copy analytical records from NZ to the BCUFR IZ.

## Author
Raphaël Rey <raphael.rey@slsp.ch>

February 27, 2026

## Usage
Update the `config.ini` configuration file with the appropriate values before running the script.

The system uses:
- SRU requests (to find NZ mms_ids)
- MongoDB (to fetch children of the parent mms_ids in the NZ)

To start the system:
`python fetch_analytical_records.py`

The script uses data included in the `data/` directory:
- `data/analytical_records.json`: Contains the analytical records to be copied.

## Installation
Install Python dependencies:
```bash
pip install -r requirements.txt
```

## License
This project is licensed under the GNU General Public License v3.0 (GPLv3). See [LICENSE](LICENSE).
