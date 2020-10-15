import io
import json
import ijson
import sqlalchemy


def create_sql_table(STATEMENT, ENGINE):
    with ENGINE.connect() as con:
        con.execute(STATEMENT)


def check_table_existance(TABLE_NAME: str, TABLE_SCHEMA: str, ENGINE):
    """Checks whether a SQL table already exists"""
    with ENGINE.connect() as con:
        result = con.execute(f"""SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{TABLE_NAME}' and TABLE_SCHEMA='{TABLE_SCHEMA}'""")
        result = [r[0] for r in result][0]
        if result == 1:
            return True
        
    return False


def gen_mysql_query(JSON_SCHEMA, DB_SCHEMA, DB_TABLE_NAME):
    """ from schema generate postgreSQL create table query"""
    data = ijson.parse(open(JSON_SCHEMA, 'r'))
    schema = {}
    # Identify all variables and their types and format. 
    for prefix, _, value in data:
        if ".type.item" in prefix or ".format" in prefix:
            key = gen_key_name(prefix)
            schema.setdefault(key, []).append(value)
            
    columns = ["id INT AUTO_INCREMENT PRIMARY KEY"]
    for k, v in schema.items():
        col_type = k
        if "date-time" in v:
            col_type += " DATETIME"
        else:
            if "string" in v:
                col_type += " TEXT"
            elif "integer" in v:
                col_type += " INT"
        
        if "null" not in v and "_" not in k:
            col_type += " NOT NULL"
        
        columns.append(col_type)
    
    columns_str = ", ".join([c for c in columns])
    create_table_query = f"""CREATE TABLE {DB_SCHEMA}.{DB_TABLE_NAME} ({columns_str});"""

    # List of column names
    columns = list(schema.keys())
    # List of all date-time variables
    datetime_columns = [k for k,v in schema.items() if "date-time" in v]

    return create_table_query, columns, datetime_columns


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


def sql_table_col_names(DB_SCHEMA, DB_TABLE, ENGINE):
    column_names = []
    with ENGINE.connect() as con:
        rows = con.execute(f"select column_name from information_schema.columns where table_schema = '{DB_SCHEMA}' and table_name='{DB_TABLE}'")
        column_names = [row[0] for row in rows]

    return column_names


def create_mysql_engine(host, port, db, user, pwd):
    """Creates a sqlalchemy engine, with specified connection information
    Arguments
    ---------
    host: string
       Host adress
    port: string
       Port for the host adress
    db: string
       Database name
    user: string
       User for connecting to database
    pwd: string
        Password for the provided user
    Returns
    -------
    engine: sqlalchemy Engine
    """
    engine = sqlalchemy.create_engine(
        "mysql+mysqldb://{}:{}@{}:{}/{}?charset=utf8".format(user, pwd, host, port, db)
    )
    return engine


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
    # Lower case keys
    cleaned_dicts = [dict((k.lower(), v) for k, v in d.items()) for d in cleaned_dicts]

    return cleaned_dicts


def write_json(data, full_path_filename): 
    with open(full_path_filename,'w') as f: 
        json.dump(data, f, indent=4) 


def read_json(full_path_filename):
    with open(full_path_filename) as json_file: 
        data = json.load(json_file) 

    return data
