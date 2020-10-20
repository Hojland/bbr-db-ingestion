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
import logging
import re
from pathlib import Path
import functools
import logging
import jmespath

from utils import utils, sql_utils
from utils import sql, datafordeler_utils
import settings

def datafordeler_initial_parser(metadata: dict, metadata_file: str):
    schema_path = settings.SCHEMA_PATH.absolute().as_posix() + '/' + metadata["schema_name"]
    
    schema = datafordeler_utils.parse_datafordeler_schema(schema_path)
    mysql_engine = sql_utils.create_engine(settings.MARIADB_CONFIG, db_name=settings.MARIADB_CONFIG['db'], db_type='mysql')
    metadata["columns"] = list(schema.keys())
    metadata["datetime_columns"] = [k for k,v in schema.items() if "date-time" in v]
    utils.write_json(metadata, metadata_file)

    base_url = settings.DATAFORDELER_BASE_URL
    url = base_url + metadata['objekttype'].lower()
    params = {
        'username': settings.DATAFORDLER_API_USR,
        'password': settings.DATAFORDLER_API_PSW,
        'page': 1,
        'pagesize': 1000,
    }
    scroll = True
    start_time = utils.time_now()
    while scroll:
        logging.info(str(utils.time_now() - start_time) + " now page: " + str(params["page"]))
        res = requests.get(url, params=params).json()
        res = datafordeler_utils.dict_flattener(res)
        # Lower case keys
        res = [dict((k.lower(), v) for k, v in d.items()) for d in res]
        df = pd.DataFrame(res, columns=metadata["columns"])
        df[metadata["datetime_columns"]] = \
            df[metadata["datetime_columns"]].apply(
            lambda x: pd.to_datetime(x, format=metadata['datetime_format'],
            errors='coerce', utc=True), axis=0)
        df.to_sql(
            name=metadata["name"], con=mysql_engine,
            index=False, schema=settings.DB_SCHEMA,
            if_exists='append', method="multi")
        scroll = True if res else False
        params["page"] += 1
        time.sleep(settings.DATAFORDELER_API_SLEEP_TIME)
        if params["page"] > 5:
            break

def datafordeler_new_events(metadata: dict):
    mysql_engine = sql_utils.create_engine(settings.MARIADB_CONFIG, db_name=settings.MARIADB_CONFIG['db'], db_type='mysql')
    latest_date = sql_utils.get_latest_date_in_table(mysql_engine, settings.DB_SCHEMA + '.' + metadata['name'], date_col='registreringfra')
    base_url = settings.DATAFORDELER_EVENTS_BASE_URL
    params = {
        'username': settings.DATAFORDLER_API_USR,
        'password': settings.DATAFORDLER_API_PSW,
        'page': 1,
        'pagesize': 1000,
        'datefrom': latest_date.strftime('%Y-%m-%d'), # virkningFra ??
        'dateto' : datetime.today().strftime('%Y-%m-%d'),
    }
    scroll = True
    start_time = utils.time_now()
    while scroll:
        logging.info(str(utils.time_now() - start_time) + " now page: " + str(params["page"]))
        res = requests.get(base_url, params=params).text
        replace_dct = {
            "æ" : "ae",
            "ø" : "oe",
            "å" : "aa",
            }
        res = utils.multiple_replace(replace_dct, res, flags=re.IGNORECASE)
        res = json.loads(res)
        
        events = jmespath.search(f'[].Message.Grunddatabesked.Haendelsesbesked.Beskedkuvert.Filtreringsdata \
                                  .{{beskedtype: beskedtype, status: to_number(Objektregistrering[0].status), \
                                   id: Objektregistrering[0].objektID, objektansvarlig: Objektregistrering[0].objektansvarligAktoer, \
                                   objekttype: Objektregistrering[0].objekttype}} \
                                    | [?contains(`{settings.DATAFORDELER_ACCEPTED_STATUSCODES}`, status)]', 
                                 res)

        for event in events:
            logging.info(f'updating event: {event}')
            metadata = get_metadata_for_objecttype(event['objekttype'])
            df = get_object(id=event['id'], metadata=metadata)
            if 'Create' in event['beskedtype']:
                df.to_sql(
                    name=metadata["name"], con=mysql_engine,
                    index=False, schema=settings.DB_SCHEMA,
                    if_exists='append')
            elif 'Update' in event['beskedtype']:
                # update date_to dates
                date_from_cols = [col for col in metadata['datetime_columns'] if 'fra' in col]
                update_dct = df.loc[0, date_from_cols]. \
                    rename(lambda x: x.replace('fra', 'til')).to_dict()
                index_dct = {'id_lokalid': event['id']}
                sql_utils.update_table(mysql_engine, settings.DB_SCHEMA + '.' + metadata['name'],
                                       update_dct, index_dct)
                df.to_sql(
                    name=metadata["name"], con=mysql_engine,
                    index=False, schema=settings.DB_SCHEMA,
                    if_exists='append')


        scroll = True if res else False
        params["page"] += 1
        time.sleep(settings.DATAFORDELER_API_SLEEP_TIME)
        if params["page"] > 5:
            break

def get_object(id: str, metadata: dict):
    base_url = settings.DATAFORDELER_BASE_URL
    url = base_url + metadata['objekttype'].lower()
    params = {
        'username': settings.DATAFORDLER_API_USR,
        'password': settings.DATAFORDLER_API_PSW,
        'Id': id,
    }
    res = requests.get(url, params).json()
    # Lower case keys
    res = [dict((k.lower(), v) for k, v in d.items()) for d in res]
    df = pd.DataFrame(res, columns=metadata['columns'])
    df[metadata["datetime_columns"]] = \
        df[metadata["datetime_columns"]].apply(
        lambda x: pd.to_datetime(x, format=metadata['datetime_format'],
        errors='coerce', utc=True), axis=0)
    return df

@functools.lru_cache
def get_metadata_for_objecttype(objekttype: str):
    for metadata_file in metadata_filelst:
        metadata = utils.read_json(metadata_file)

        if metadata['objekttype'] == objekttype:
            return metadata
        else:
            pass


def main():
    logger = utils.get_logger('printyboi.log')
    # Load in source meta data
    metadata_filelst = glob.glob(settings.METADATA_PATH.absolute().as_posix() + '/*.json')
    mysql_engine = sql_utils.create_engine(settings.MARIADB_CONFIG, db_name=settings.MARIADB_CONFIG['db'], db_type='mysql')
    
    for metadata_file in metadata_filelst:
        metadata = utils.read_json(metadata_file)
        if not sql_utils.table_exists(mysql_engine, settings.DB_SCHEMA, metadata["name"]):
            """Ingest a brand new table into database"""
            datafordeler_initial_parser(metadata, metadata_file)
        else:
            """Ingest new events into database"""
            datafordeler_new_events(metadata)


if __name__ == '__main__':
    main()