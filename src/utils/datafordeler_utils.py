import ijson
import json
import io

def parse_datafordeler_schema(datafordeler_json_schema: dict):
    data = ijson.parse(open(datafordeler_json_schema, 'r'))
    schema = {}
    # Identify all variables and their types and format. 
    for prefix, _, value in data:
        if value and "properties" in prefix and (".type" in prefix or ".format" in prefix):
            print(prefix, value)
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