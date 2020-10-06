import ijson
import psycopg2
from psycopg2 import Error

def create_sql_table(QUERY, USER, PASSWORD, HOST, PORT, DATABASE):
    try:
        connection = psycopg2.connect(user = USER, 
                                        password = PASSWORD, 
                                        host = HOST, 
                                        port = PORT, 
                                        database = DATABASE)
        cursor = connection.cursor()
        cursor.execute(QUERY)
        connection.commit()
    
    except (Exception, psycopg2.DatabaseError) as error :
        print ("Error while creating PostgreSQL table", error)
    
    finally:
        if(connection):
            cursor.close()
            connection.close()


def gen_postgre_query(JSON_SCHEMA, DB_SCHEMA, DB_TABLE_NAME):
    """ from schema generate postgreSQL create table query"""
    data = ijson.parse(open(JSON_SCHEMA, 'r'))
    schema = {}
    # Identify all variables and their types and format. 
    for prefix, event, value in data:
        if ".type.item" in prefix or ".format" in prefix:
            key = gen_key_name(prefix)
            schema.setdefault(key, []).append(value)
            
    columns = ["id serial PRIMARY KEY"]
    for k, v in schema.items():
        col_type = k
        if "date-time" in v:
            col_type += " TIMESTAMP"
        else:
            if "string" in v:
                col_type += " TEXT"
            elif "integer" in v:
                col_type += " INT"
        
        if "null" not in v:
            col_type += " NOT NULL"
        
        columns.append(col_type)
    
    columns_str = ", ".join([c for c in columns])
    create_table_query = f"""CREATE TABLE {DB_SCHEMA}.{DB_TABLE_NAME} ({columns_str});"""

    return create_table_query


def gen_key_name(s: str):
    """Given a json path with seperator=. generate a key name. """
    s_l = s.split(".")
    pos = [i for i, x in enumerate(s_l) if x == "properties"]
    last_prop = max(pos)
    key_name = s_l[last_prop+1]
    
    if s_l[last_prop-1] != "items":
        key_name = s_l[last_prop-1] + "_" + key_name

    return key_name


def sql_table_col_names(DB_TABLE, DB_SCHEMA, USER, PASSWORD, HOST, PORT, DATABASE):
    column_names = []
    with psycopg2.connect(user = USER, 
                            password = PASSWORD, 
                            host = HOST,
                            port = PORT,
                            database = DATABASE) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"select column_name from information_schema.columns where table_schema = '{DB_SCHEMA}' and table_name='{DB_TABLE}'")
            column_names = [row[0] for row in cursor]

    return column_names

    