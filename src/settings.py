import os
from pathlib import Path

MARIADB_CONFIG = {
    "user": os.environ["MARIADB_USR"],
    "psw": os.environ["MARIADB_PSW"],
    "host": "cubus.cxxwabvgrdub.eu-central-1.rds.amazonaws.com",
    "port": 3306,
    "db": "bbr",
}

#POSTGRESDB_CONFIG = {
#    "user": os.environ["POSTGRES_USR"],
#    "psw": os.environ["POSTGRES_PSW"],
#    "host": "localhost",
#    "port": 5432,
#    "db": "dev-db",
#}

DATAFORDELER_BASE_URL = 'https://services.datafordeler.dk//BBR/BBRPublic/1/REST/'
DATAFORDELER_EVENTS_BASE_URL = 'https://services.datafordeler.dk/system/EventMessages/1.0.0/custom?'
DATAFORDELER_API_USR = os.environ["DATAFORDELER_API_USR"]
DATAFORDELER_API_PSW = os.environ["DATAFORDELER_API_PSW"]
DATAFORDELER_API_SLEEP_TIME = 2
DATAFORDELER_ACCEPTED_STATUSCODES = ['6', '7','8', '9', '10', '11']
DATAFORDELER_CODELIST_URL = 'https://teknik.bbr.dk/kodelister/0/1/0/'
DATAFORDLER_API_PAGESIZE = 5000

SCHEMA_PATH = Path('schemas')
METADATA_PATH = Path('metadata')

DB_SCHEMA = 'bbr'