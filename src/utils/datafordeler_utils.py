import ijson
import json
import io
import numpy as np
import sqlalchemy
import pandas as pd
import functools
import jmespath
import re
import requests
from bs4 import BeautifulSoup
import aiohttp
from aiohttp import ClientSession
import aiomysql
import asyncio

from utils import sql, utils, sql_utils
import settings

replace_characters_dct = {
    "æ" : "ae",
    "Æ" : "ae",
    "ø" : "oe",
    "Ø" : 'oe',
    "å" : "aa",
    'Å' : 'aa',
    'tek070datoforsenestudfoertesupplerendeindvendigkorrosionsbeskyttelse': 'tek070datoindvendigkorrosionsbeskyttelse',
    }

def parse_datafordeler_schema(datafordeler_json_schema: str):
    data = ijson.parse(open(datafordeler_json_schema, 'r'))
    schema = {}
    # Identify all variables and their types and format. 
    for prefix, _, value in data:
        if value and "properties" in prefix and (".type" in prefix or ".format" in prefix):
            key = gen_key_name(prefix)
            schema.setdefault(key, []).append(value)

    schema = {key:value for (key,value) in schema.items() if "object" not in value and "array" not in value}
    return schema


def gen_key_name(s: str):
    """Given a json path with seperator=. generate a key name. """
    s_l = s.split(".")
    pos = [i for i, x in enumerate(s_l) if x == "properties"]
    last_prop = max(pos)
    key_name = s_l[last_prop+1]
    
    if s_l[last_prop-1] != "items":
        key_name = s_l[last_prop-1] + "_" + key_name

    return key_name.lower()


def gen_key_name_request(s: str):
    """Given a json path with seperator=. generate a key name for datafordeler api requests """
    s_l = s.split(".")
    pos = [i for i, x in enumerate(s_l) if x == "item"]
    last_item = max(pos)
    key_name = s_l[last_item+1:]
    key_name = "_".join(key_name).lower()
    return key_name

def dict_flattener(d: str):
    """Function that flattens a dict with nested dicts using ijson.
        Each nested dict variable will be prefixed with <itemname>_"""

    bytestring = str.encode(json.dumps(d))
    # convert dict to ijson format
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

    return cleaned_dicts

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
async def get_codelist(session: ClientSession, url: str):
    res = await session.get(url)
    res = await res.text()
    soup = BeautifulSoup(res, features="html.parser")
    codelist = {}
    for code in soup.select('.kodeliste-list li'):
        key, value = code.text.split(' - ', maxsplit=1)
        codelist[key] = value
    return codelist

def codelist_exceptions(codelist_options: list):
    codelist_txt = json.dumps(codelist_options)
    replace_dct = {
        "byganvendelse" : "bygningensanvendelse",
        "forretningshaendelse": "forretningshaendelsekodeliste",
        'forretningsomraade': "forretningsomraadekodeliste",
        "ejerforholdskode": "ejendommensejerforholdskode",
        'ydervaeggenes': 'ydervaeggens',
        'adressefunktion': 'adresserolle',
        'forretningsprocess': 'forretningsproces'
    }
    codelist_txt = utils.multiple_replace(replace_dct, codelist_txt)
    codelist_options = json.loads(codelist_txt)
    return codelist_options

async def create_codelist_dims(schema_name: str):
    queue = asyncio.Queue()
    
    async def get_and_create_codelist(session: ClientSession, mysql_engine_pool, code: dict, queue: asyncio.Queue):
        sleeptime  = queue.get_nowait()
        await asyncio.sleep(sleeptime)
        dim_table = await get_codelist(session, code['url'])
        await sql.create_codelist_dim_table(mysql_engine_pool, settings.DB_SCHEMA, code['option'], dim_table)
        queue.task_done()
    session = ClientSession()
    loop = asyncio.get_event_loop()
    mysql_engine_pool = await sql_utils.async_mysql_create_engine(loop=loop, db_config=settings.MARIADB_CONFIG, db_name=settings.MARIADB_CONFIG['db'])
    codelist = get_codelist_options()
    codelist = codelist_exceptions(codelist)
    tasks = []
    for code in codelist:
        queue.put_nowait(1 * queue.qsize())
        task = asyncio.create_task(get_and_create_codelist(session, mysql_engine_pool, code, queue))
        tasks.append(task)
        await queue.join()
        


async def translate_codes(session: ClientSession, df: pd.DataFrame):
    codelist_options = get_codelist_options()
    codelist_options = codelist_exceptions(codelist_options)
    col_parts = [option['option'] for option in codelist_options]
    short_df_col = [re.search('(?<=\d{3})(.*)', col).group(0) if re.search('\d{3}', col) else col for col in list(df)]
    col_translate = dict(zip(short_df_col, list(df)))
    for col in col_parts:
        if col in short_df_col:
            url = jmespath.search(f"[?option=='{col}'].url | [0]", codelist_options)
            codelist_translater = await get_codelist(session, url)
            df[col_translate[col]] = df[col_translate[col]].replace(codelist_translater)
    return df

async def correct_df_according_to_datatype_limits(mysql_engine_pool: aiomysql.Pool, df: pd.DataFrame, schema_name: str, table_name: str):
    col_dtypes = await sql_utils.col_dtypes(mysql_engine_pool, schema_name, table_name)
    for col, dtype in col_dtypes.items():
        if dtype == 'int' and col!='id':
            if df[col].dtype=='object':
                df[col] = df[col].where(df[col].str.contains('\d'), np.nan)
    return df

def other_ridiculous_value_exceptions(df: pd.DataFrame):
    if 'bestemtfastejendom_ejendommensejerforholdskode' in list(df):
        df['bestemtfastejendom_ejendommensejerforholdskode'] = \
            df['bestemtfastejendom_ejendommensejerforholdskode'].replace({'99': '90'})
    return df