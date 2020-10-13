import requests
import ast
import pandas as pd
import math
import ijson
import os
import time
import json
from configparser import ConfigParser
from datetime import datetime
# Use sqlalchemy instead
from src.utils import create_sql_table, gen_mysql_query, sql_table_col_names, gen_key_name_request, create_mysql_engine, dict_flattener, write_json, read_json

start_time = time.time()
# Load in source meta data
path_to_src_metadata = 'src/metadata/bbr_enhed.json'
metadata = read_json(path_to_src_metadata)


# Get config file
config_object = ConfigParser()
config_object.read("config.ini")
serverconfig = config_object["SQLSERVERCONFIG"]

# Database configurations
db_input_config = {
"host": serverconfig["host"],
"port": serverconfig["port"],
"db": serverconfig["db"],
"user": os.getenv("MARIADB_USER"),
"pwd": os.getenv("MARIADB_PW")
}

# Create database engine
input_engine = create_mysql_engine(**db_input_config)

path_to_json = metadata["schema_path"]
# Create postgre query
create_table_query, columns, datetime_columns = gen_mysql_query(path_to_json, db_input_config["db"], metadata["name"])
# create_sql_table(create_table_query, input_engine)
# Create pd dataframe consistent with sql table
metadata["columns"] = columns
metadata["datetime_columns"] = datetime_columns 
write_json(metadata, path_to_src_metadata)

# Get data from source API
rest_url = metadata["endpoint_all"]
pars = {'username': 'XBNOBAOZNU', 
'password': 'HejHej-1234', "page": 2, "pagesize": 1000}
# Request data
output = requests.get(rest_url, params=pars).text
# Flatten dict
output = dict_flattener(output)
# dataframe it
df = pd.DataFrame(output, columns = columns)
# Convert datecolumns to python datetimes
for datecol in datetime_columns:
    df.loc[:, datecol] = df.loc[:, datecol].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f%z") if pd.notnull(x) else x)


df.to_sql(name=metadata["name"], con=input_engine, index=False, schema=db_input_config["db"], if_exists='append', method="multi")

print("--- %s seconds ---" % (time.time() - start_time))



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

pars = {'username': 'XBNOBAOZNU', 
'password': 'HejHej-1234', "page": 2, "pagesize": 3608}

# Request data
output = requests.get(bbr_enhed_url, params=pars).text
# Flatten dict
output = dict_flattener(output)
# Lowercase all keys to be consistent with postgresql.

# Create dataframe that's consistent with db table structure. 
bbr_enhed = pd.DataFrame(output, columns = column_names)

# Push to DB
bbr_enhed.to_sql(name="bbr_enhed", con=input_engine, index=False, schema='input', if_exists='append', method="multi")

# Pull hændelser BBR
"https://services.datafordeler.dk/system/EventMessages/1.0.0/custom?datefrom=2020-01-01&dateto=2020-02-01&username=<some_username>&password=<some_password>&format=Json&page=1&pagesize=1000"


from datetime import datetime
d = datetime.strptime(bbr_enhed.iloc[-10, 0], "%Y-%m-%dT%H:%M:%S.%f%z")
d = d.replace(tzinfo=None)


bbr_enhed.iloc[-10, 0]

s[0:10]


d.strftime("%Y-%m-%dT%H:%M:%S")






### Generating configs
#Get the configparser object
config_object = ConfigParser()

#Assume we need 2 sections in the config file, let's call them USERINFO and SERVERCONFIG
config_object["SQLSERVERCONFIG"] = {
    "host": "cubus.cxxwabvgrdub.eu-central-1.rds.amazonaws.com",
    "port": "3306",
    "db": "input"
}

#Write the above sections to config.ini file
with open('config.ini', 'w') as conf:
    config_object.write(conf)


config_object = ConfigParser()
config_object.read("config.ini")

config_object["SQLSERVERCONFIG"]["host"]