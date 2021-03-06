import io
import json
import ijson
import sqlalchemy
import pytz
from datetime import datetime, timedelta
import logging
import time
import sys
import re


def write_json(data, full_path_filename): 
    with open(full_path_filename,'w') as f: 
        json.dump(data, f, indent=4) 


def read_json(full_path_filename):
    with open(full_path_filename) as json_file: 
        data = json.load(json_file) 
    return data

def get_logger(log_name: str='/app/logs/hello.log'):
    """Creates new logger.
    Args:
        model_name (str):
            Folder name for the logger to be saved in.
            Accepted values: 'ncf', 'implicit_model'
        model_dir (str): Name of the logger file.
    Returns:
        logger: Logger object.
    """

    def copenhagen_time(*args):
        """Computes and returns local time in Copenhagen.
        Returns:
            time.struct_time: Time converted to CEST.
        """
        _ = args  # to explicitly remove warning
        utc_dt = pytz.utc.localize(datetime.utcnow()) + timedelta(minutes=5, seconds=30)
        local_timezone = pytz.timezone("Europe/Copenhagen")
        converted = utc_dt.astimezone(local_timezone)
        return converted.timetuple()

    logging.Formatter.converter = copenhagen_time
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()

    # To files
    fh = logging.FileHandler(log_name)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)
    logger.setLevel(logging.INFO)

    # to std out
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' % \
                  (method.__name__, (te - ts) * 1000))
        return result
    return timed

def time_now(local_tz: pytz.timezone=pytz.timezone("Europe/Copenhagen")):
    now = datetime.today().replace(tzinfo=pytz.utc).astimezone(tz=local_tz)
    return now

def multiple_replace(replace_dct: dict, text: str, **kwargs):
  # Create a regular expression  from the dictionary keys
  regex = re.compile("(%s)" % "|".join(map(re.escape, replace_dct.keys())), **kwargs)

  # For each match, look-up corresponding value in dictionary
  return regex.sub(lambda mo: replace_dct[mo.string[mo.start():mo.end()]], text)

def mark_list_duplicates(lst: list):
    return [True if lst.count(col)>1 else False for col in lst]

def split_list(lst: list, chunk_size: int):
    return [lst[offs:offs+chunk_size] for offs in range(0, len(lst), chunk_size)]

# keeping this not because we need it but because of inspo
#lookup_cols = [short_cols_db[i] + short_cols[i] + short_cols[:i].count(short_cols[i]) if duplicates[i] else short_cols[i] for i in range(len(short_cols))]