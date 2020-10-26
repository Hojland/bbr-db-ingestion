import sqlalchemy

def create_sql_table(engine: sqlalchemy.engine, datafordeler_schema: dict, schema_name: str, table_name: str):
    """ from schema generate postgreSQL create table query"""

    columns = ["id INT AUTO_INCREMENT PRIMARY KEY"]
    for k, v in datafordeler_schema.items():
        col_type = k
        if "date-time" in v:
            col_type += " DATETIME"
        elif "string" in v:
            col_type += " TEXT"
        elif "integer" in v:
            col_type += " INT"
        elif 'number' in v:
            col_type += " FLOAT"
        
        if "null" not in v and "_" not in k:
            col_type += " NOT NULL"
        
        columns.append(col_type)

    columns_str = ", ".join(columns)
    create_table_query = f"""CREATE TABLE {schema_name}.{table_name} ({columns_str});"""
    
    with engine.connect() as con:
        con.execute(create_table_query)

def sql_table_col_names(engine: sqlalchemy.engine, schema_name: str, table_name: str):
    column_names = []
    with engine.connect() as con:
        rows = con.execute(f"select column_name from information_schema.columns where table_schema = '{schema_name}' and table_name='{table_name}'")
        column_names = [row[0] for row in rows]

    return column_names