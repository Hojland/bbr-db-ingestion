import requests
import ast
import pandas as pd
import math
import ijson
import io
# Use sqlalchemy instead
import psycopg2
from bbr_ingestion.utils import create_sql_table, gen_postgre_query, sql_table_col_names, gen_key_name_request


# DB info
user = "nli"
password = "lotus"
host = "127.0.0.1"
port = "5432"
database = "dev-db"

path_to_json = "bbr_ingestion/schemas/BBR_2.4.4_Enhed.schema.json"

# Create postgre query
create_table_query = gen_postgre_query(path_to_json, "bbr", "enhed")
# Create table in postgre DB
create_sql_table(create_table_query, user, password, host, port, database)
# Create pd dataframe consistent with sql table
column_names = sql_table_col_names("enhed", "bbr", user, password, host, port, database)
column_names.remove("id")


# Extracting data from data fordeler. 

# Dokumentation for BBR 
# https://confluence.datafordeler.dk/pages/viewpage.action?pageId=16056582

# All URLs for BBR
# bbr_bygning_url = "https://services.datafordeler.dk//BBR/BBRPublic/1/REST/bygning?"
bbr_enhed_url = "https://services.datafordeler.dk//BBR/BBRPublic/1/REST/enhed?"
# bbr_ejendomsrelation_url = "https://services.datafordeler.dk//BBR/BBRPublic/1/REST/ejendomsrelation?"
# br_sag_url = "https://services.datafordeler.dk//BBR/BBRPublic/1/REST/bbrsag?"
# bbr_grund_url = "https://services.datafordeler.dk//BBR/BBRPublic/1/REST/grund?"
# bbr_tekniskanlaeg_url = "https://services.datafordeler.dk//BBR/BBRPublic/1/REST/tekniskanlaeg?"

# Tjenestbruger credentials and paging for http request
#pars = {'username': 'XBNOBAOZNU', 
#'password': 'HejHej-1234', "page": 2, "pagesize": 3608}
pars = {'username': 'XBNOBAOZNU', 
'password': 'HejHej-1234', "page": 2, "pagesize": 3608}

# Request data
output = requests.get(bbr_enhed_url, params=pars)
output = output.text
# Lowercase all keys to be consistent with postgresql.
output = [dict((k.lower(), v) for k,v in d.items()) for d in output]
# Create dataframe that's consistent with db table structure. 
bbr_enhed = pd.DataFrame(output, columns = column_names)
bbr_enhed.isna().mean()


# Extract 
bytestring = str.encode(output)
parse_events = ijson.parse(io.BytesIO(bytestring))

cleaned_dicts = []

# Extract all variables. 
for prefix, event, value in parse_events:
    if (prefix, event, value) == ("item", "start_map", None):
        d = {}
    elif "item." in prefix and event in ["number", "string"]:
        d[gen_key_name_request(prefix)] = value
    elif (prefix, event, value) == ("item", "end_map", None):
        cleaned_dicts.append(d)
        del d
    else:
        pass


# Pull hændelser BBR 
"https://services.datafordeler.dk/system/EventMessages/1.0.0/custom?datefrom=2020-01-01&dateto=2020-02-01&username=<some_username>&password=<some_password>&format=Json&page=1&pagesize=1000"
