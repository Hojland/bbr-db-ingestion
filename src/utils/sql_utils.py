import sqlalchemy
import pandas as pd
import os
from tqdm import tqdm
import sys
from sqlalchemy.types import String, Integer, Numeric
from os.path import exists, join, abspath
import logging
import re

from utils import utils


def create_engine(db_config: dict, db_name: str=None, db_type: str='postgres'):
    """Creates a sqlalchemy engine, with specified connection information

    Arguments
    ---------
    db_config: a dictionary with configurations for a resource
    db_name: Overwrite the database from the config
    db_type: a string with the database type for prepending the connection string

    Returns
    -------
    engine: sqlalchemy.Engine
    """
    if db_type == 'postgres':
       prepend = 'postgresql+psycopg2'
    elif db_type == 'mssql':
       prepend = 'mssql+pyodbc'
    elif db_type == 'mysql' or db_type == 'mariadb':
       prepend = 'mysql+mysqldb'

    uid, psw, host, port, db = db_config.values()
    if db_name:
       db = db_name
    conn_string = f"{prepend}://{uid}:{psw}@{host}:{port}/{db}"

    if db_type == 'mssql':
        driverfile = '/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so'
        conn_string = conn_string + f"?DRIVER={driverfile};TDS_VERSION=7.2"
    elif db_type == 'mysql':
        conn_string = conn_string + '?charset=utf8'

    engine = sqlalchemy.create_engine(conn_string)
    return engine


def get_latest_date_in_table(db_engine: sqlalchemy.engine, table_name: str, date_col: str='date'):
    latest_date = db_engine.execute(f'SELECT MAX({date_col}) FROM {table_name}').scalar()
    if not latest_date:
        raise IndexError(f"No data in variable '{date_col}' in table")
    return latest_date

def delete_index_from_table(db_engine: sqlalchemy.engine, index_dct: dict, table_name: str):
    index_string = ' AND '.join(f"{key}='{value}'" for key, value in index_dct.items())
    sql_query = f'DELETE FROM output.{table_name} WHERE {index_string}'
    db_engine.execute(sql_query)

def delete_date_entries_in_table(db_engine: sqlalchemy.engine, min_date: str, table_name: str):
    db_engine.execute(f'DELETE FROM output.{table_name} WHERE date>="{min_date}";')

def delete_table(db_engine: sqlalchemy.engine, table: str):
    db_engine.execute(f'DROP TABLE {table}')

def truncate_table(db_engine: sqlalchemy.engine, table: str):
    db_engine.execute(f'TRUNCATE TABLE {table}')

def table_exists(db_engine: sqlalchemy.engine, schema_name: str, table_name: str):
    exists_num = db_engine.execute(f'''
    SELECT EXISTS (SELECT * 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema_name}' 
        AND  TABLE_NAME = '{table_name}')
    ''').scalar()
    if exists_num == 0:
        exists = False
    elif exists_num == 1:
        exists = True
    return exists

def update_table(db_engine: sqlalchemy.engine, table_name: str, update_dct: dict, index_dct: dict):
    update_string = ', '.join(f"{key}='{value}'" for key, value in update_dct.items())
    replace_dct = {
        "'nat'": 'NULL',
        "'nan'": 'NULL',
    }
    update_string = utils.multiple_replace(replace_dct, update_string, flags=re.IGNORECASE)
    index_string = ' AND '.join(f"{key}='{value}'" for key, value in index_dct.items())
    sql_query = f'UPDATE {table_name} SET {update_string} WHERE {index_string}'
    db_engine.execute(sql_query)

def view_exists(db_engine: sqlalchemy.engine, schema: str, view: str, sql_lang: str='mysql'):
    base_sql_query = f'''EXISTS (SELECT * 
                         FROM INFORMATION_SCHEMA.VIEWS
                         WHERE TABLE_SCHEMA = '{schema}' 
                         AND  TABLE_NAME = '{view}')'''

    if sql_lang == 'mysql':
        exists_num = db_engine.execute(f'''
        SELECT {base_sql_query}
        ''').scalar()
    elif sql_lang == 'mssql':
        exists_num = db_engine.execute(f'''
        SELECT
            CASE
                WHEN
                    {base_sql_query}
                    THEN 1 
                ELSE 0 
            END
        ''').scalar()
    if exists_num == 0:
        exists = False
    elif exists_num == 1:
        exists = True
    return exists

def table_empty(db_engine: sqlalchemy.engine, table: str):
   empty_num = db_engine.execute(f'''
      SELECT EXISTS(SELECT 1 FROM {table})
   ''').scalar()
   if empty_num == 0:
      empty = False
   elif empty_num == 1:
      empty = True
   return empty

def table_exists_empty(db_engine: sqlalchemy.engine, schema: str, table: str):
   exists = table_exists(db_engine, schema, table)
   if exists:
      empty = table_empty(db_engine, schema + '.' + table)
      if empty:
         both = True
      else:
         both = False
   else:
      both = False
   return both

def table_index_exists(db_engine: sqlalchemy.engine, schema: str, table: str, index_name: str=None):
    sql_query = f'''
    SELECT COUNT(1) as IndexIsThere FROM INFORMATION_SCHEMA.STATISTICS
    WHERE table_schema='{schema}' AND table_name='{table}'
    '''
    if index_name:
        sql_query = sql_query + f" AND index_name='{index_name}'"

    index_exists_num = db_engine.execute(sql_query).scalar()
    if index_exists_num == 0:
        index_exists = False
    elif index_exists_num > 0:
        index_exists = True
    else:
        index_exists = False
    return index_exists

def col_dtypes(db_engine: sqlalchemy.engine, schema_name: str, table_name: str):
    res = db_engine.execute(f"SELECT column_name, data_type FROM information_schema.columns where table_schema = '{schema_name}' and table_name='{table_name}'")
    col_dtypes = {column_name: data_type for column_name, data_type in res}
    return col_dtypes

def load_data(engine: sqlalchemy.engine, sql_query: str):
    df_load = pd.read_sql(sql_query, engine, chunksize=20000)
    try:
        df = pd.concat([chunk for chunk in tqdm(df_load, desc='Loading data', file=sys.stdout)], ignore_index=True)
    except ValueError:
        logging.error('No data in sql query table')
        df = pd.DataFrame()
    return df


def get_dtype_trans(df: pd.DataFrame, str_len: int=50):
    obj_vars = [colname for colname in list(df) if df[colname].dtype == 'object']
    int_vars = [colname for colname in list(df) if df[colname].dtype == 'int64']
    float_vars = [colname for colname in list(df) if df[colname].dtype == 'float64']

    dtype_trans = {
        obj_var: String(str_len) for obj_var in obj_vars
    }
    dtype_trans.update({
        int_var: Integer for int_var in int_vars
    })
    dtype_trans.update({
        float_var: Numeric(14, 5) for float_var in float_vars
    })
    return dtype_trans