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
from   fileutils import *
from   sqlitedb import SQLiteDB
from   dorunrun import dorunrun, ExitCode
from   urlogger import URLogger
###
# imports and objects that are a part of this project
###
import os
import argparse
import random
import time
import signal
import pandas as pd
import json
import io
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

actions = {'freq':{'lower': 1,
                   'upper': 1440,
                   'e_msg': 'Frequency cannot be accepted'}}




# For static data
caught_signals = [  signal.SIGINT, signal.SIGQUIT,                     
                    signal.SIGUSR1, signal.SIGUSR2, signal.SIGTERM ]
logger=URLogger(level=logging.DEBUG, logfile="collector.log")
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


def dither_time(t:int) -> int:
    """
    Avoid measuring the power at regular intervals
    """
    lower = int(t * 0.95)
    upper = int(t * 1.05)
    while True:
        yield random.randint(lower, upper)

def legit_freq(value):
    min_freq = 1  # 1 minute
    max_freq = 1440  # 1 day
    ivalue = int(value)
    if ivalue < min_freq or ivalue > max_freq:
        raise argparse.ArgumentTypeError(f"Frequency must be between {min_freq} and {max_freq} minutes.")
    return ivalue

def file_exists(value):
    if not os.path.isfile(value):
        raise argparse.ArgumentTypeError(f"File {value} does not exist.")
    return value
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
        result = dorunrun(cmd, return_datatype=dict)
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
        df.rename(columns={df.columns[0]: 'indexs'}, inplace=True)
    except Exception as e:
        return e
    
    return df
@trap
def build_facts(df:pd.DataFrame, db:object, ff:io.TextIOWrapper) -> None:
    """
    This function builds FACTS table for cluster built by ACT company
    The FACTs table includes cv_stats datum
    
    The purpose is to write pandas DataFrame to SQLite3
    :param df: the pandas DataFrame  
    :param db: the database connection    
    """

    try:
        # Melt the DataFrame to long format
        df = df.melt(id_vars=['indexs'], var_name='nodenames', value_name='datum')
    
        # For TABLE FACTS
        sql_facts = """INSERT INTO FACTS(indexs, devices, datum)
                   VALUES (?, ?, ?)"""
        # Read the interested indexs from the file
        interested_indexs= list(read_whitespace_file(ff))
        df = df[df['indexs'].isin(interested_indexs)]
        facts_values = df[['indexs','nodenames','datum']]
        rows_affected_facts = db.executemany_SQL(sql_facts, facts_values)
        logger.debug(f"Rows affected in facts table: {rows_affected_facts}")
        
    except Exception as e:
        print(e)
    finally:
        db.commit()

@trap
def collector_main(myargs:argparse.Namespace) -> int:
    global db_handle
    
    db_handle = db = SQLiteDB(myargs.db)
    myargs.verbose and print(f"Database {myargs.db} open")
   
    error = 0 
    n=0
    # Break time unit is minute
    dither_iter = dither_time(myargs.freq * 60)
    
    while not error and n < myargs.n:
        n += 1
        # df_value
        value_cmd = "sudo cv-stats -a --format=json"
        value = collect_datum(value_cmd)
        df_value = filter_datum(value)
        
        error = build_facts(df_value, db, myargs.file)
        time.sleep(next(dither_iter))
    
    return os.EX_OK


if __name__ == '__main__':
    here       = os.getcwd()
    progname   = os.path.basename(__file__)[:-3]
    configfile = f"{here}/{progname}.toml"
    lookupkey  = "lookup"
    database   = "clusterdata.db"
    logfile    = f"{here}/{progname}.log"
    
    parser = argparse.ArgumentParser(prog="collector", 
        description="What collector does, collector does best.")

    parser.add_argument('-o', '--output', type=str, default="",
        help="Output file name")
    parser.add_argument('--key', type=str, default=lookupkey, 
        help=f"Key to filter the data dictionary, defaults to {lookupkey}")
    parser.add_argument('--db', type=file_exists, default=database, 
        help=f"Name of database, defaults to {database}")
    parser.add_argument('--file', type=file_exists, required=True,
        help="Name of the file to filter interested cv_stats data")
    parser.add_argument('-f', '--freq', type=legit_freq, default=10,
        help='number of minute between polls (default:60)')
    parser.add_argument('-n', type=int, default=sys.maxsize,
        help="For debugging, limit number of readings (default:unlimited)")
    parser.add_argument('-z', '--zap', action='store_true', 
        help=f"Remove {logfile} and create a new one.")
    myargs = parser.parse_args()
    if myargs.zap:
        try:
            os.unlink(logfile)
        finally:
            pass

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
            exit_code = ExitCode(collector_main(myargs))
            print(exit_code)
            sys.exit(int(exit_code))
    except Exception as e:
        print(f"Escaped or re-raised exception: {e}")

