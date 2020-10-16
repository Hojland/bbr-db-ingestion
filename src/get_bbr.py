%load_ext autoreload
%autoreload 2
import requests
import ast
import pandas as pd
import math
import ijson
import os
import time
import json
from datetime import datetime
import glob
from pathlib import Path

from utils import utils, sql_utils
from utils import sql, datafordeler_utils
import settings

start_time = utils.time_now()

def datafordeler_initial_parser(metadata: dict, metadata_file: str):
    schema_path = settings.SCHEMA_PATH.absolute().as_posix() + '/' + metadata["schema_name"]
    
    schema = datafordeler_utils.parse_datafordeler_schema(schema_path)
    mysql_engine = sql_utils.create_engine(settings.MARIADB_CONFIG, db_name=settings.MARIADB_CONFIG['db'], db_type='mysql')
    metadata["columns"] = list(schema.keys())
    metadata["datetime_columns"] = [k for k,v in schema.items() if "date-time" in v]
    utils.write_json(metadata, metadata_file)

    base_url = metadata['endpoint_all']
    params = {
        'username': settings.DATAFORDLER_API_USR,
        'password': settings.DATAFORDLER_API_PSW,
        'page': 1,
        'pagesize': 1000,
    }
    res = output = requests.get(base_url, params=params).json()
    res = datafordeler_utils.dict_flattener(res)
    df = pd.DataFrame(res, columns=metadata["columns"])

    df[metadata["datetime_columns"]] = df[metadata["datetime_columns"]].apply(lambda x: pd.to_datetime(x, format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce', utc=True), axis=0)
    return df

def datafordeler_new_events(metadata: dict, metadata_file: str):

def main():
    # Load in source meta data
    metadata_filelst = glob.glob(settings.METADATA_PATH.absolute().as_posix() + '/*.json')
    mysql_engine = sql_utils.create_engine(settings.MARIADB_CONFIG, db_name=settings.MARIADB_CONFIG['db'], db_type='mysql')
    
    for metadata_file in metadata_filelst:
        metadata = utils.read_json(metadata_file)
        if not sql_utils.table_exists(mysql_engine, settings.MARIADB_CONFIG['db'], metadata["name"]):
            """Ingest a brand new table into database"""
            df = datafordeler_initial_parser(metadata_file, metadata)
            df.to_sql(name=metadata["name"], con=mysql_engine, index=False, schema=settings.MARIADB_CONFIG['db'], if_exists='append', method="multi")
        else:
            """Ingest new events into database"""
            df = datafordeler_new_events(etadata_file, metadata)


if __name__ == '__main__':
    main()

#TODO:
# Change schemas to a settings var instead of DB from settings


# Create database engine
input_engine = utils.create_mysql_engine(**db_input_config)

# Check table existance
if utils.check_table_existence(metadata["name"], db_input_config["db"], input_engine):
    #update table with events
else:
    # Initialize the beast!
    schema_path = metadata["schema_path"]
    db_schema = utils.parse_datafordeler_schema(schema_path)

    # List of column names
    columns = list(schema.keys())
    # List of all date-time variables
    datetime_columns = [k for k,v in schema.items() if "date-time" in v]

    create_table_query, columns, datetime_columns = utils.gen_mysql_query(path_to_json, db_input_config["db"], metadata["name"])
    utils.create_sql_table(create_table_query, input_engine)

    # Create pd dataframe consistent with sql table
    metadata["columns"] = columns
    metadata["datetime_columns"] = datetime_columns 
    utils.write_json(metadata, path_to_src_metadata)
    
    # Get data from source API
    rest_url = metadata["endpoint_all"]
    pars = {'username': 'XBNOBAOZNU', 
    'password': 'HejHej-1234', "page": 1, "pagesize": 1000}
    # Request data
    output = requests.get(rest_url, params=pars).text
    # Flatten dict
    output = utils.dict_flattener(output)
    # dataframe it
    df = pd.DataFrame(output, columns = columns)
    # Convert datecolumns to python datetimes
    
    for datecol in datetime_columns:
        df.loc[:, datecol] = df.loc[:, datecol].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f%z").strftime("%Y-%m-%d %H:%M:%S.%f") if pd.notnull(x) else x)
    
    df.to_sql(name=metadata["name"], con=input_engine, index=False, schema=db_input_config["db"], if_exists='append', method="multi")


import requests #to make TMDB API calls


output = requests.get(rest_url, params=pars).json()
output_2 = utils.dict_flattener(output.json())



if output.json():
    print("hej")

output = True

requests.get(rest_url, params={"count":True, 'username': 'XBNOBAOZNU', 
'password': 'HejHej-1234'}).text

4171641 / 1000







rest_url = metadata["endpoint_all"]
pars = {'username': 'XBNOBAOZNU', 
'password': 'HejHej-1234', "page": 1, "pagesize": 1000}

t = time.time()
output = True
a = []
while output:
    print(str(time.time() - t) + " now page: " + str(pars["page"]))
    output = requests.get(rest_url, params=pars).json()
    output = utils.dict_flattener(output)
    df = pd.DataFrame(output, columns = columns)
    # Convert datecolumns to python datetimes
    for datecol in datetime_columns:
        df.loc[:, datecol] = df.loc[:, datecol].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f%z").strftime("%Y-%m-%d %H:%M:%S.%f") if pd.notnull(x) else x)
    
    df.to_sql(name=metadata["name"], con=input_engine, index=False, schema=db_input_config["db"], if_exists='append', method="multi")
    pars["page"] += 1
    print(str(time.time() - t) + " next page: " + str(pars["page"]))
    time.sleep(2)






427 * 4175 / 60 / 60 / 


339.6647219657898 - 427.5229036808014

( 90 * 4175 ) / 60 / 60 / 24

output

a = pd.DataFrame(output, columns=metadata["columns"])

a.to_sql(name=metadata["name"], con=input_engine, index=False, schema=db_input_config["db"], if_exists='append', method="multi")

    # 
    output = requests.get(rest_url, params=pars).json
    # Flatten dict
    output = utils.dict_flattener(output)
    # dataframe it
    df = pd.DataFrame(output, columns = columns)
    # Convert datecolumns to python datetimes
    for datecol in datetime_columns:
        df.loc[:, datecol] = df.loc[:, datecol].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f%z").strftime("%Y-%m-%d %H:%M:%S.%f") if pd.notnull(x) else x)
    
    df.to_sql(name=metadata["name"], con=input_engine, index=False, schema=db_input_config["db"], if_exists='append', method="multi")
    pars["page"] += 1



print("--- %s seconds ---" % (time.time() - start_time))


output.json()
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
output = utils.dict_flattener(output)
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