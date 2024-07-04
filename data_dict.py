# -*- coding: utf-8 -*-
import typing
from   typing import *
min_py = (3, 8)

###
# Standard imports, starting with os and sys
###
import os
import sys
if sys.version_info < min_py:
     print(f"This program requires Python {min_py[0]}.{min_py[1]}, or higher.")
     sys.exit(os.EX_SOFTWARE)
###
# Other standard distro imports
###
import argparse
import contextlib
import getpass
mynetid = getpass.getuser()
import logging

###
# From hpclib
###
import linuxutils
from   urdecorators import trap
from   urlogger import URLogger
from   fileutils import *
from   sqlitedb import SQLiteDB
from   dorunrun import dorunrun, ExitCode
###
# imports and objects that are a part of this project
###
import os
import argparse
import random
import signal
import pandas as pd
import time
import json
verbose = False

###
# Credits
###
__author__ = 'Skyler He'
__copyright__ = 'Copyright 2024, University of Richmond'
__credits__ = None
__version__ = 0.1
__maintainer__ = 'Skyler He'
__email__ = 'skyler.he@richmond.edu', 'yingxinskyler.he@gmail.com'
__status__ = 'in progress'
__license__ = 'MIT'

# For static data
caught_signals = [  signal.SIGINT, signal.SIGQUIT,                     
                    signal.SIGUSR1, signal.SIGUSR2, signal.SIGTERM ]

logger=URLogger(level=logging.DEBUG, logfile="datadict.log") 

# For signal handler
db_handle = None
def handler(signum:int, stack:object=None) -> None:
    """
    Universal signal handler
    """
    if signum == signal.SIGHUP:
        return
    if signum in caught_signals:
        try:
            db_handle.commit()
            db_handle.db.close()
        except Exception as e:
            sys.stderr.write(f"Error on exit {e}\n")
            sys.exit(os.EX_IOERR)
        else:
            sys.exit(os.EX_OK)
    else:
        return

@trap
def collect_datum(cmd:str) -> dict:
    """
    This function executes a Linux command through the dorunrun function and expects
    the output to be a nested dictionary from the standard output (stdout).

    :param cmd: The Linux command to be executed.
    :return: A dictionary containing the parsed JSON output from stdout. If an error occurs,
             a dictionary with the key 'Error' and the exception message is returned.
    """
    try:
        result = dorunrun(cmd, return_datatype = "json")
    except Exception as e:
        return {'Error':e}
    else: 
        return json.loads(result['stdout'])


@trap
def filter_datum(data:dict, key:str = None) -> pd.DataFrame:
    """
    This function converts a dictionary to a pandas DataFrame.
    If a key is specified, it converts the dictionary at that key.
    If the key does not exist or is not specified, it converts the entire dictionary.

    :param data: The input dictionary to convert.
    :param key: The key to filter the dictionary on.
    :return: A DataFrame.
    """
    try:
        df = pd.DataFrame(data[key]).T
    except KeyError:
        df = pd.DataFrame(data).T
        df.reset_index(inplace = True)
        df.rename(columns={df.columns[0]: 'index'}, inplace=True)
    except Exception as e:
        return e
    
    return df

@trap
def build_datadict(df:pd.DataFrame, db:object) -> None:
    """
    This function builds data dictionary for cluster built by ACT company. 
    The data dictionary includes the general description of cv_stats data.
    The purpose is to write pandas DataFrame to SQLite3
    :param df: the pandas DataFrame
    :param db: the database connection
    """
    try:
        
        # Create df['indexs'] based on stat_type and name
        df['indexs'] = df['stat_type'].str.lower() + '.' + df['name'].str.lower()
        # For TABLE data_dictionary 
        sql_ddict = """INSERT INTO data_dictionary(indexs, statstypes, names, types, methods, units, precision)
               VALUES (?, ?, ?, ?, ?, ?, ?)"""
        ddict_values = df[['indexs', 'stat_type', 'name', 'type', 'method', 'unit', 'precision']]
        rows_affected_ddict = db.executemany_SQL(sql_ddict, ddict_values)
        logger.debug(f"Rows affected in data_dictionary table: {rows_affected_ddict}")


        logger.debug("Data dictionary built successfully.")
    except Exception as e:
        print(e)
    finally:
        db.commit()    

@trap
def data_dict_main(myargs:argparse.Namespace) -> int:
    global db_handle
    # df_def     
    def_cmd = "/usr/sbin/cv-cockpit-helper --stat-definition"
    defs = collect_datum(def_cmd)
    df_def = filter_datum(defs, myargs.key)
    
    try:
        db_handle = db = SQLiteDB(myargs.db)
        build_datadict(df_def, db)
    except Exception as e:
        print(e)
    finally:
        db.commit()  
    return os.EX_OK


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog="datacollector", 
        description="What datacollector does, datacollector does best.")

    parser.add_argument('-i', '--input', type=str, default="",
        help="Input file name.")
    parser.add_argument('-o', '--output', type=str, default="",
        help="Output file name")
    parser.add_argument('-v', '--verbose', action='store_true',
        help="Be chatty about what is taking place")
    parser.add_argument('--key', type=str, default="lookup", 
        help="Key to filter the data dictionary")
    parser.add_argument('--db', type=str, default="clusterdata.db", 
        help="Name of database")
    myargs = parser.parse_args()
    verbose = myargs.verbose
    
    for _ in caught_signals:
         try:
             signal.signal(_, handler)
         except OSError as e:
             myargs.verbose and sys.stderr.write(f"Cannot reassign signal {_}\n")
         else:
             myargs.verbose and sys.stderr.write(f"Signal {_} is being handled.\n")


    try:
        outfile = sys.stdout if not myargs.output else open(myargs.output, 'w')
        with contextlib.redirect_stdout(outfile):
            sys.exit(globals()[f"{os.path.basename(__file__)[:-3]}_main"](myargs))

    except Exception as e:
        print(f"Escaped or re-raised exception: {e}")

