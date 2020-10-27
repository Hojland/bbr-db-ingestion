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
from datetime import datetime, timedelta
import glob
import logging
import re
from pathlib import Path
import functools
import logging
import jmespath
from bs4 import BeautifulSoup

from utils import utils, sql_utils
from utils import sql, datafordeler_utils
import settings

@functools.lru_cache
def get_codelist_options():
    res = requests.get(settings.DATAFORDELER_CODELIST_URL).content
    soup = BeautifulSoup(res, features="html.parser")
    codelist_options = []
    for option in soup.select('option')[1:]:
        codelist_options.append(
            {'option': option.text.lower(),
             'url': option['value']}
            )
    return codelist_options

@functools.lru_cache
def get_codelist(url: str):
    res = requests.get(url).content
    soup = BeautifulSoup(res, features="html.parser")
    codelist = {}
    for code in soup.select('.kodeliste-list li'):
        key, value = code.text.split(' - ', maxsplit=1)
        codelist[key] = value
    return codelist

def codelist_exceptions(codelist_options: list):
    codelist_txt = json.dumps(codelist_options)
    replace_dct = {
        "byganvendelse" : "bygningsanvendelse"
    }
    codelist_txt = utils.multiple_replace(codelist_txt, res, flags=re.IGNORECASE)
    codelist_options = json.loads(codelist_txt)
    return codelist_options

def translate_codes(df: pd.DataFrame):
    codelist_options = get_codelist_options()
    codelist_options = codelist_exceptions(codelist_options)
    col_parts = [option['option'] for option in codelist_options]
    short_df_col = [re.search('(?<=\d{3})(.*)', col).group(0) if re.search('\d{3}', col) else col for col in list(df)]
    col_translate = dict(zip(short_df_col, list(df)))
    for col in col_parts:
        if col in short_df_col:
            url = jmespath.search(f"[?option=='{col}'].url | [0]", codelist_options)
            codelist_translater = get_codelist(url)
            df[col_translate[col]] = df[col_translate[col]].replace(codelist_translater)
    return df

    #for col in list(df):
    #    match = map(col.__contains__, col_parts)
    #    match_idx = [i for i, x in enumerate(match) if x]
    #    if match_idx:
    #        codelist_url_dct = codelist_options[match_idx[0]]
    #        codelist_translater = get_codelist(codelist_url_dct['url'])
    #        print(f"col: {col}, codelist_translater: {codelist_translater}, option: {col_parts[match_idx[0]]}")
    #        df[col] = df[col].replace(codelist_translater)

def datafordeler_initial_parser(metadata: dict, metadata_file: str):
    schema_path = settings.SCHEMA_PATH.absolute().as_posix() + '/' + metadata["schema_name"]
    
    schema = datafordeler_utils.parse_datafordeler_schema(schema_path)
    mysql_engine = sql_utils.create_engine(settings.MARIADB_CONFIG, db_name=settings.MARIADB_CONFIG['db'], db_type='mysql')
    metadata["columns"] = list(schema.keys())
    metadata["datetime_columns"] = [k for k,v in schema.items() if "date-time" in v]
    utils.write_json(metadata, metadata_file)

    sql.create_sql_table(mysql_engine, schema, settings.DB_SCHEMA, metadata['name'])

    base_url = settings.DATAFORDELER_BASE_URL
    url = base_url + metadata['objekttype'].lower()
    params = {
        'username': settings.DATAFORDLER_API_USR,
        'password': settings.DATAFORDLER_API_PSW,
        'page': 1,
        'pagesize': 5000,
        'status': '|'.join(settings.DATAFORDELER_ACCEPTED_STATUSCODES)
    }
    scroll = True
    start_time = utils.time_now()
    while scroll:
        logging.info(str(utils.time_now() - start_time) + " now page: " + str(params["page"]))
        res = requests.get(url, params=params).text
        replace_dct = {
            "æ" : "ae",
            "Æ" : "ae",
            "ø" : "oe",
            "Ø" : 'oe',
            "å" : "aa",
            'Å' : 'aa',
            'tek070datoforsenestudfoertesupplerendeindvendigkorrosionsbeskyttelse': 'tek070datoindvendigkorrosionsbeskyttelse',
            }
        res = utils.multiple_replace(replace_dct, res, flags=re.IGNORECASE)
        res = json.loads(res)
        res = datafordeler_utils.dict_flattener(res)
        # Lower case keys
        res = [dict((k.lower(), v) for k, v in d.items()) for d in res]
        #dtypes = get_dtypes_dct(schema_dct=schema, columns=metadata['columns'])
        df = pd.DataFrame(res, columns=metadata["columns"])
        df = df.convert_dtypes()
        df[metadata["datetime_columns"]] = to_datetime_format(df[metadata["datetime_columns"]], format=metadata['datetime_format'])
        df = translate_codes(df)
        df.to_sql(
            name=metadata["name"], con=mysql_engine,
            index=False, schema=settings.DB_SCHEMA,
            if_exists='append')
        scroll = True if res else False
        params["page"] += 1
        time.sleep(settings.DATAFORDELER_API_SLEEP_TIME)
        if params["page"] > 100:
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
            "Æ" : "ae",
            "ø" : "oe",
            "Ø" : 'oe',
            "å" : "aa",
            'Å': 'aa',
            'tek070datoforsenestudfoertesupplerendeindvendigkorrosionsbeskyttelse': 'tek070datoindvendigkorrosionsbeskyttelse',
            }
        res = utils.multiple_replace(replace_dct, res, flags=re.IGNORECASE)
        res = json.loads(res)
        
        events = jmespath.search('[].Message.Grunddatabesked.Haendelsesbesked.Beskedkuvert.Filtreringsdata \
                                  .{beskedtype: beskedtype, status: to_number(Objektregistrering[0].status), \
                                   id: Objektregistrering[0].objektID, objektansvarlig: Objektregistrering[0].objektansvarligAktoer, \
                                   objekttype: Objektregistrering[0].objekttype}', 
                                 res)

        # We will use the above instead. When making changs, we should accept all statuscodes and react to them. Other than that we should just 
        # subscribe to the correct stuff on datafordeler.dk
        #events = jmespath.search(f'[].Message.Grunddatabesked.Haendelsesbesked.Beskedkuvert.Filtreringsdata \
        #                          .{{beskedtype: beskedtype, status: to_number(Objektregistrering[0].status), \
        #                           id: Objektregistrering[0].objektID, objektansvarlig: Objektregistrering[0].objektansvarligAktoer, \
        #                           objekttype: Objektregistrering[0].objekttype}} \
        #                            | [?contains(`{settings.DATAFORDELER_ACCEPTED_STATUSCODES}`, status)]', 
        #                         res)

        for event in events:
            logging.info(f'updating event: {event}')
            metadata = get_metadata_for_objecttype(event['objekttype'])
            schema_path = settings.SCHEMA_PATH.absolute().as_posix() + '/' + metadata["schema_name"]
            schema = datafordeler_utils.parse_datafordeler_schema(schema_path)
            df = get_object(id=event['id'], metadata=metadata, schema=schema)
            df = translate_codes(df)
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
        if params["page"] > 100:
            break

def get_object(id: str, metadata: dict, schema=dict):
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
    #dtypes = get_dtypes_dct(schema_dct=schema, columns=metadata['columns'])
    df = pd.DataFrame(res, columns=metadata['columns'])
    df = df.convert_dtypes()
    df[metadata["datetime_columns"]] = to_datetime_format(df[metadata["datetime_columns"]], format=metadata['datetime_format'])
    return df

def get_dtypes_dct(schema_dct: dict, columns: list):
    dtypes_trans_dct = {
        'date-time': 'object',
        'integer': 'int64',
        'string': 'object',
        'number': 'float64',
        'array': 'object',
    }
    dtypes = [dtypes_trans_dct[v[0]] for k,v in schema_dct.items()]
    dtypes = dict(zip(columns, dtypes))
    return dtypes

def to_datetime_format(datetime_dataframe: pd.DataFrame, format: str=None):
    #datetime_dataframe = \
    #    datetime_dataframe.apply(
    #    lambda x: x.where(x > '1970-01-01', '1970-01-01'), axis=0)
    #datetime_dataframe = \
    #    datetime_dataframe.apply(
    #    lambda x: x.where(x < (datetime.today() + timedelta(days=10*365)).strftime('%Y-%m-&d'),
    #            (datetime.today() + timedelta(days=10*365)).strftime('%Y-%m-&d')), axis=0)
    datetime_dataframe = \
        datetime_dataframe.apply(
        lambda x: pd.to_datetime(x, format=format,
        errors='coerce', utc=True).dt.strftime("%Y-%m-%d %H:%M:%S.%f"), axis=0)
    return datetime_dataframe

@functools.lru_cache
def get_metadata_for_objecttype(objekttype: str):
    metadata_filelst = glob.glob(settings.METADATA_PATH.absolute().as_posix() + '/*.json')
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
            pass
        """Ingest new events into database"""
        datafordeler_new_events(metadata)



if __name__ == '__main__':
    main()