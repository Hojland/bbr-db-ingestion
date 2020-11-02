#%load_ext autoreload
#%autoreload 2
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
import asyncio
import aiohttp
from aiohttp import ClientSession

from utils import utils, sql_utils
from utils import sql, datafordeler_utils
import settings

def get_count(url: str):
    params = {
        'username': settings.DATAFORDLER_API_USR,
        'password': settings.DATAFORDLER_API_PSW,
        'count': True
    }
    res = requests.get(url, params=params).json()
    return res['count']

async def request_json(session: ClientSession, url: str, params: dict):
    res = await session.get(url, params=params)
    res = await res.text()
    res = utils.multiple_replace(datafordeler_utils.replace_characters_dct, res, flags=re.IGNORECASE)
    res = json.loads(res)
    res = datafordeler_utils.dict_flattener(res)
    res = [dict((k.lower(), v) for k, v in d.items()) for d in res]
    return res

async def request_and_ingest(mysql_engine_pool, session: ClientSession, params: dict, url: str, metadata: dict, queue):
    sleeptime  = queue.get_nowait()
    logging.info(f"Trying to sleep {sleeptime} for my little task")
    await asyncio.sleep(sleeptime)
    logging.info(f"Awake again and now requesting the suckers")
    res = await request_json(session, url, params)
    logging.info(f"Got my result, good!")
    scroll = True if res else False
    if not res:
        return scroll
    df = pd.DataFrame(res, columns=metadata["columns"])
    df = await datafordeler_utils.correct_df_according_to_datatype_limits(mysql_engine_pool, df, settings.DB_SCHEMA, metadata["name"])
    df = datafordeler_utils.other_ridiculous_value_exceptions(df)
    df = df.convert_dtypes()
    df[metadata["datetime_columns"]] = to_datetime_format(df[metadata["datetime_columns"]], format=metadata['datetime_format'])
    logging.info(f"awaiting sending to database for page: {params['page']}")
    await sql_utils.df_to_sql(mysql_engine_pool, df, f'{settings.DB_SCHEMA}.{metadata["name"]}')
    logging.info(f"I have just put some data into {metadata['name']}")
    queue.task_done()
    return scroll

async def datafordeler_initial_parser(metadata: dict, metadata_file: str):
    schema_path = settings.SCHEMA_PATH.absolute().as_posix() + '/' + metadata["schema_name"]
    schema = datafordeler_utils.parse_datafordeler_schema(schema_path)

    metadata["columns"] = list(schema.keys())
    metadata["datetime_columns"] = [k for k,v in schema.items() if "date-time" in v]
    utils.write_json(metadata, metadata_file)

    loop = asyncio.get_event_loop()
    mysql_engine_pool = await sql_utils.async_mysql_create_engine(loop=loop, db_config=settings.MARIADB_CONFIG, db_name=settings.MARIADB_CONFIG['db'])
    
    await sql.create_sql_table(mysql_engine_pool, schema, metadata, settings.DB_SCHEMA, metadata['name'])

    base_url = settings.DATAFORDELER_BASE_URL
    url = base_url + metadata['objekttype'].lower()

    count = get_count(url)
    pages_count = math.ceil(count/settings.DATAFORDLER_API_PAGESIZE)
    params = {
        'username': settings.DATAFORDLER_API_USR,
        'password': settings.DATAFORDLER_API_PSW,
        'page': 0,
        'pagesize': settings.DATAFORDLER_API_PAGESIZE,
        'status': '|'.join(settings.DATAFORDELER_ACCEPTED_STATUSCODES),
    }
    start_time = utils.time_now()
    queue = asyncio.Queue()
    tasks = []
    session = ClientSession()
    while params["page"] < 33: #pages_count
        params["page"] += 1
        logging.info(f'Page {params["page"]} of {pages_count} getting a total of {count} rows. {str(utils.time_now() - start_time).split(".")[0]} has passed')
        
        logging.info(f'Trying to setup queue with {settings.DATAFORDELER_API_SLEEP_TIME * queue.qsize()} sleeptime')
        queue.put_nowait(settings.DATAFORDELER_API_SLEEP_TIME * queue.qsize())
        task = asyncio.create_task(request_and_ingest(mysql_engine_pool, session, params.copy(), url, metadata, queue)) # scroll implemented as failure proofing
        tasks.append(task)

        if queue.qsize() > 10:
            logging.info(f'Waiting {queue.qsize() * settings.DATAFORDELER_API_SLEEP_TIME} seconds to add to the queue')
            # await asyncio.gather(*tasks, return_exceptions=True)
            logging.info(f'Are we here?')
            await queue.join()
            #await asyncio.sleep(queue.qsize() * settings.DATAFORDELER_API_SLEEP_TIME)

#        if not scroll:
#            logging.error("You should never see me")

    if not queue.empty():
        logging.info(f'Waiting {queue.qsize() * settings.DATAFORDELER_API_SLEEP_TIME} seconds to go to next table')
        await asyncio.sleep(queue.qsize() * settings.DATAFORDELER_API_SLEEP_TIME)


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
        res = utils.multiple_replace(datafordeler_utils.replace_characters_dct, res, flags=re.IGNORECASE)
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
            df = get_object(mysql_engine, id=event['id'], metadata=metadata, schema=schema)
            #df = datafordeler_utils.translate_codes(df)
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

def get_object(mysql_engine, id: str, metadata: dict, schema=dict):
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
    df = datafordeler_utils.correct_df_according_to_datatype_limits(mysql_engine, df, settings.DB_SCHEMA, metadata["name"])
    df = datafordeler_utils.other_ridiculous_value_exceptions(df)
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

    asyncio.run(datafordeler_utils.create_codelist_dims(settings.DB_SCHEMA))
    for metadata_file in metadata_filelst:
        metadata = utils.read_json(metadata_file)
        if not sql_utils.table_exists(mysql_engine, settings.DB_SCHEMA, metadata["name"]):
            """Ingest a brand new table into database"""
            asyncio.run(datafordeler_initial_parser(metadata, metadata_file))
        else:
            pass
        """Ingest new events into database"""
        #datafordeler_new_events(metadata)

if __name__ == '__main__':
    main()

# https://stackoverflow.com/questions/29571671/basic-multiprocessing-with-while-loop

# TODO
# events should be async as well (remove requests and mysqlclient)
# events should be sent in lists of the same type, then grouped and then ingested
# put the whole shebang into production