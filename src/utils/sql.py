import sqlalchemy
import re
from collections import ChainMap
import jmespath
import aiomysql
from aiohttp import ClientSession
import asyncio

from utils import datafordeler_utils, utils

async def create_sql_table(mysql_engine_pool: aiomysql.pool, datafordeler_schema: dict, metadata: dict, schema_name: str, table_name: str):
    """ from schema generate postgreSQL create table query"""
    session = ClientSession()
    async def get_foreign_key_index_str(session: ClientSession, columns: list):
        async def codelist_id_int(session: ClientSession, url: str):
            codelist = await datafordeler_utils.get_codelist(session, url)
            return all([re.search('\d{1,3}', k) for k, v in codelist.items()])

        codelist = datafordeler_utils.get_codelist_options()
        codelist = datafordeler_utils.codelist_exceptions(codelist)

        no_preprend_cols = [col.split('_')[len(col.split('_'))-1] if not col.split('_')[0]=='id' else col for col in columns]
        short_cols_db = [re.search('(.*)(?=\d{3})', col).group(0) if re.search('\d{3}', col) else '' for col in no_preprend_cols]
        short_cols = [re.search('(?<=\d{3})(.*)', col).group(0) if re.search('\d{3}', col) else col for col in no_preprend_cols]
        duplicates = utils.mark_list_duplicates(short_cols)
        lookup_cols = [short_cols_db[i] + short_cols[i] if duplicates[i] else short_cols[i] for i in range(len(short_cols))]
        new_key_translater = {columns[i]: lookup_cols[i] for i in range(len(columns))}
        foreign_key_lst = [col for col in columns if new_key_translater[col] in jmespath.search('[].option', codelist)]
        datatype_lst = [{code['option']:'INT'} if await codelist_id_int(session, code['url']) else {code['option']: 'VARCHAR(4)'} for code in codelist if code['option'] in lookup_cols]
        datatype_dct = dict(ChainMap(*datatype_lst))
        
        foreign_key_datatypes = {foreign_key: datatype_dct[new_key_translater[foreign_key]] for foreign_key in foreign_key_lst}
        foreign_keys = {foreign_key:f"{new_key_translater[foreign_key]}_dim({new_key_translater[foreign_key]}_id)" for foreign_key in foreign_key_lst}
        foreign_key_strs = [f'FOREIGN KEY ({k}) REFERENCES {v}' for k,v in foreign_keys.items()]
        foreign_str = ", ".join(foreign_key_strs)
        index_str = ", ".join([f'INDEX ({foreign_key})' for foreign_key in foreign_key_lst])
        return foreign_str, index_str, foreign_key_datatypes

    def get_column_definition_str(schema_dct: dict, columns: list, foreign_key_datatypes: dict):
        def get_datatype_dct(schema_dct: dict, columns: list, foreign_key_datatypes: dict):
            def datatype_from_value(v: list, datatype_trans_dct: dict):
                """returns the first value in dict if in the list"""
                return [datatype for k, datatype in datatype_trans_dct.items() if k in v][0]

            def not_null_from_value(k: str, v: list):
                """returns NOT NULL if null is in the list"""
                return '' if 'null' in v or '_' in k else 'NOT NULL'

            def replace_first_word_with_dct(word: str, replace: str):
                return re.sub(r'(^.*?)(?=\s|$)', replace, word)

            datatype_trans_dct = {
                'date-time': 'DATETIME',
                'integer': 'BIGINT',
                'string': 'TEXT',
                'number': 'FLOAT',
                'array': 'TEXT',
            }
            base_datatypes = [(datatype_from_value(v, datatype_trans_dct) + ' ' + not_null_from_value(k, v)).rstrip() for k,v in schema_dct.items()]
            base_datatype_dct = dict(zip(columns, base_datatypes))

            datatype_dct = [{k: v} if k not in foreign_key_datatypes.keys() else {k: replace_first_word_with_dct(v, foreign_key_datatypes[k])} for k,v in base_datatype_dct.items()]
            datatype_dct = dict(ChainMap(*datatype_dct))
            return datatype_dct
    
        datatype_dct = get_datatype_dct(schema_dct, columns, foreign_key_datatypes)

        primary_key = "id INT AUTO_INCREMENT PRIMARY KEY, "
        col_definition_str = primary_key + ', '.join([f"{k} {v}" for k, v in datatype_dct.items()])
        return col_definition_str

     
    foreign_str, index_str, foreign_key_datatypes = await get_foreign_key_index_str(session, metadata['columns'])
    col_definition_str = get_column_definition_str(datafordeler_schema, metadata['columns'], foreign_key_datatypes)
    
    create_table_query = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({col_definition_str}, {index_str}, {foreign_str});"""

    conn = await mysql_engine_pool.acquire()
    cur = await conn.cursor()
    await cur.execute(create_table_query)

def sql_table_col_names(engine: sqlalchemy.engine, schema_name: str, table_name: str):
    column_names = []
    with engine.connect() as con:
        rows = con.execute(f"select column_name from information_schema.columns where table_schema = '{schema_name}' and table_name='{table_name}'")
        column_names = [row[0] for row in rows]
    return column_names

async def create_codelist_dim_table(mysql_engine_pool: aiomysql.Pool, schema_name: str, option: str, dim_table: dict):
    is_int = all([re.search('\d{1,3}', k) for k, v in dim_table.items()])
    datatype = 'INT' if is_int else 'VARCHAR(4)'
    create_definition = f"""{option}_id {datatype} NOT NULL, text VARCHAR(350), PRIMARY KEY({option}_id)"""
    sql_query = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{option}_dim ({create_definition})"""
    
    conn = await mysql_engine_pool.acquire()
    cur = await conn.cursor()
    await cur.execute(sql_query)
    
    col_list_str = ','.join([f'{option}_id', 'text'])
    row_values_str = ','.join([f"('{k}', '{v}')" for k,v in dim_table.items()])
    sql_query = f"""REPLACE INTO {schema_name}.{option}_dim ({col_list_str}) VALUES {row_values_str}"""
    
    await cur.execute(sql_query)

    await cur.close()
    await mysql_engine_pool.release(conn)
