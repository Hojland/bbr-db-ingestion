#%load_ext autoreload
#%autoreload 2
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
import aiomysql

from utils import utils, sql_utils
from utils import sql, datafordeler_utils
import settings

async def get_count(session: ClientSession, url: str):
    params = {
        'username': settings.DATAFORDELER_API_USR,
        'password': settings.DATAFORDELER_API_PSW,
        'count': 'True'
    }
    res = await session.get(url, params=params)
    res = await res.json()
    return res['count']

async def request_replace_json(session: ClientSession, url: str, params: dict):
    res = await session.get(url, params=params)
    assert res.status == 200, f"Status for request is {res.status} with reason '{res.reason}' for page {params['page']}"
    res = await res.text()
    res = utils.multiple_replace(datafordeler_utils.replace_characters_dct, res, flags=re.IGNORECASE)
    res = json.loads(res)
    return res

async def request_and_ingest(mysql_engine_pool, session: ClientSession, params: dict, url: str, metadata: dict, queue):
    sleeptime  = queue.get_nowait()
    logging.info(f"Trying to sleep {sleeptime} for my little task")
    await asyncio.sleep(sleeptime)
    logging.info(f"Awake again and now requesting the suckers")
    try:
        res = await request_replace_json(session, url, params)
    except AssertionError as ae:
        logging.info(f"Error in request {ae}")
        queue.task_done()
        return True
    res = datafordeler_utils.dict_flattener(res)
    res = [dict((k.lower(), v) for k, v in d.items()) for d in res]

    logging.info(f"Got my result, good!")
    scroll = True if res else False
    if not res:
        queue.task_done()
        return scroll
    df = pd.DataFrame(res, columns=metadata["columns"])
    df = await datafordeler_utils.correct_df_according_to_datatype_limits(mysql_engine_pool, df, settings.DB_SCHEMA, metadata["name"])
    df = datafordeler_utils.other_ridiculous_value_exceptions(df)
    df = df.convert_dtypes()
    df[metadata["datetime_columns"]] = to_datetime_format(df[metadata["datetime_columns"]], format=metadata['datetime_format'])
    logging.info(f"awaiting sending to database for page: {params['page']}")
    await sql_utils.df_to_sql_split(mysql_engine_pool, df, f'{settings.DB_SCHEMA}.{metadata["name"]}')
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
    session = ClientSession()

    count = await get_count(session, url)
    pages_count = math.ceil(count/settings.DATAFORDELER_API_PAGESIZE)
    params = {
        'username': settings.DATAFORDELER_API_USR,
        'password': settings.DATAFORDELER_API_PSW,
        'page': 0,
        'pagesize': settings.DATAFORDELER_API_PAGESIZE,
        'status': '|'.join(settings.DATAFORDELER_ACCEPTED_STATUSCODES),
    }
    start_time = utils.time_now()
    queue = asyncio.Queue()
    scroll = True
    while params["page"] < pages_count and scroll:
        params["page"] += 1
        logging.info(f'Page {params["page"]} of {pages_count} getting a total of {count} rows. {str(utils.time_now() - start_time).split(".")[0]} has passed')
        
        logging.info(f'Trying to setup queue with {settings.DATAFORDELER_API_SLEEP_TIME * queue.qsize()} sleeptime')
        queue.put_nowait(settings.DATAFORDELER_API_SLEEP_TIME * queue.qsize())
        scroll = await request_and_ingest(mysql_engine_pool, session, params.copy(), url, metadata, queue) # scroll implemented as failure proofing

        if queue.qsize() > 10:
            logging.info(f'Waiting {queue.qsize() * settings.DATAFORDELER_API_SLEEP_TIME} seconds to add to the queue')
            # await asyncio.gather(*tasks, return_exceptions=True)
            await queue.join()
            #await asyncio.sleep(queue.qsize() * settings.DATAFORDELER_API_SLEEP_TIME)

    if not queue.empty():
        logging.info(f'Waiting {queue.qsize() * settings.DATAFORDELER_API_SLEEP_TIME} seconds to go to next table')
        await asyncio.sleep(queue.qsize() * settings.DATAFORDELER_API_SLEEP_TIME)

# https://stackoverflow.com/questions/46890646/asyncio-weirdness-of-task-exception-was-never-retrieved

async def datafordeler_new_events():
    loop = asyncio.get_event_loop()
    mysql_engine_pool = await sql_utils.async_mysql_create_engine(loop=loop, db_config=settings.MARIADB_CONFIG, db_name=settings.MARIADB_CONFIG['db'])
    
    latest_date = await sql_utils.get_latest_date_in_table(mysql_engine_pool, f"{settings.DB_SCHEMA}.bygning", date_col='registreringfra')
    base_url = settings.DATAFORDELER_EVENTS_BASE_URL
    session = ClientSession()
    params = {
        'username': settings.DATAFORDELER_API_USR,
        'password': settings.DATAFORDELER_API_PSW,
        'page': 1,
        'pagesize': settings.DATAFORDELER_API_PAGESIZE,
        'datefrom': latest_date.strftime('%Y-%m-%d'), # virkningFra ??
        'dateto' : datetime.today().strftime('%Y-%m-%d'),
    }

    scroll = True
    start_time = utils.time_now()
    queue = asyncio.Queue()
    while scroll:
        params["page"] += 1
        logging.info(f'Page {params["page"]}. {str(utils.time_now() - start_time).split(".")[0]} has passed')

        queue.put_nowait(settings.DATAFORDELER_API_SLEEP_TIME * queue.qsize())
        sleeptime  = queue.get_nowait()
        await asyncio.sleep(sleeptime)
        res = await request_replace_json(session, base_url, params)
        
        events = jmespath.search('[].Message.Grunddatabesked.Haendelsesbesked.Beskedkuvert.Filtreringsdata \
                                  .{beskedtype: beskedtype, status: to_number(Objektregistrering[0].status), \
                                   id: Objektregistrering[0].objektID, objektansvarlig: Objektregistrering[0].objektansvarligAktoer, \
                                   objekttype: Objektregistrering[0].objekttype}', 
                                res)
        events = organise_events(events)

        for objecttype, message_type_id in events.items():
            metadata = get_metadata_for_objecttype(objecttype)
            schema_path = settings.SCHEMA_PATH.absolute().as_posix() + '/' + metadata["schema_name"]
            schema = datafordeler_utils.parse_datafordeler_schema(schema_path)
            for message_type, ids in message_type_id.items():
                if not ids:
                    continue
                try:
                    df = await get_object(mysql_engine_pool, session=session, ids=ids, metadata=metadata, schema=schema)
                except AssertionError as ae:
                    logging.info(f'got http error {ae} for {objecttype} and {message_type}')
                    break
                if message_type == 'Create':
                    await sql_utils.df_to_sql(mysql_engine_pool, df, f'{settings.DB_SCHEMA}.{metadata["name"]}')
                elif message_type == 'Update':
                    # update date_to dates
                    date_from_cols = [col for col in metadata['datetime_columns'] if 'fra' in col]
                    update_df = df[date_from_cols]. \
                        rename(lambda x: x.replace('fra', 'til'), axis=1)
                    index_df = df['id_lokalid']
                    await sql_utils.several_updates_table(mysql_engine_pool, f"{settings.DB_SCHEMA}.{ metadata['name']}",
                                                          update_df, index_df)
                    await sql_utils.df_to_sql(mysql_engine_pool, df, f'{settings.DB_SCHEMA}.{metadata["name"]}')

        queue.task_done()

        scroll = True if res else False

        if queue.qsize() > 10:
            logging.info(f'Waiting {queue.qsize() * settings.DATAFORDELER_API_SLEEP_TIME} seconds to add to the queue')
            await queue.join()

            if not queue.empty():
                logging.info(f'Waiting {queue.qsize() * settings.DATAFORDELER_API_SLEEP_TIME} seconds to go to next table')
                await asyncio.sleep(queue.qsize() * settings.DATAFORDELER_API_SLEEP_TIME)


def organise_events(events: dict):
    object_types = list(set(jmespath.search('[].objekttype', events)))
    message_types = ['Create', 'Update']
    organised_events = {}
    for object_type in object_types:
        filtered_events = jmespath.search(f"[?objekttype == '{object_type}'].{{id: id, beskedtype: beskedtype}}", events)
        object_type_dct = {}
        for message_type in message_types:
            object_type_dct[message_type] = jmespath.search(f"[?contains(beskedtype, '{message_type}')].id", filtered_events)
            organised_events[object_type] = object_type_dct
    return organised_events

async def get_object(mysql_engine_pool: aiomysql.pool, session: ClientSession, ids: list, metadata: dict, schema=dict):
    base_url = settings.DATAFORDELER_BASE_URL
    url = base_url + metadata['objekttype'].lower()

    ids_lst = utils.split_list(ids, 100)
    res_lst = []
    for hundred_ids in ids_lst:
        params = {
            'username': settings.DATAFORDELER_API_USR,
            'password': settings.DATAFORDELER_API_PSW,
            'Id': '|'.join(hundred_ids),
        }
        res = await session.get(url, params=params)
        assert res.status == 200, f"Status for request is {res.status} with reason {res.reason}"
        res = await res.json()
        res_lst.extend(res)
    # Lower case keys
    res = [dict((k.lower(), v) for k, v in d.items()) for d in res_lst]
    #dtypes = get_dtypes_dct(schema_dct=schema, columns=metadata['columns'])
    df = pd.DataFrame(res, columns=metadata['columns'])
    df = await datafordeler_utils.correct_df_according_to_datatype_limits(mysql_engine_pool, df, settings.DB_SCHEMA, metadata["name"])
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

    # asyncio.run(datafordeler_utils.create_codelist_dims(settings.DB_SCHEMA))

    for metadata_file in metadata_filelst:
        metadata = utils.read_json(metadata_file)
        table_exists_empty = asyncio.run(sql_utils.table_exists_empty(schema=settings.DB_SCHEMA, table_name=metadata["name"]))
        if not table_exists_empty:
            """Ingest a brand new table into database"""
            asyncio.run(datafordeler_initial_parser(metadata, metadata_file))

    """Ingest new events into database"""
    asyncio.run(datafordeler_new_events())

if __name__ == '__main__':
    main()

# https://stackoverflow.com/questions/29571671/basic-multiprocessing-with-while-loop