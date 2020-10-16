import os
from pathlib import Path

MARIADB_CONFIG = {
    "user": os.environ["MARIADB_USR"],
    "psw": os.environ["MARIADB_PSW"],
    "host": "cubus.cxxwabvgrdub.eu-central-1.rds.amazonaws.com",
    "port": 3306,
    "db": "input",
}

POSTGRESDB_CONFIG = {
    "user": os.environ["POSTGRES_USR"],
    "psw": os.environ["POSTGRES_PSW"],
    "host": "localhost",
    "port": 5432,
    "db": "dev-db",
}

DATAFORDLER_API_USR=os.environ["DATAFORDLER_API_USR"]
DATAFORDLER_API_PSW=os.environ["DATAFORDLER_API_PSW"]

SCHEMA_PATH = Path('schemas')
METADATA_PATH = Path('metadata')