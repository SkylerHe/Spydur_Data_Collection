# -*- coding: utf-8 -*-
from   typing import *


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

###
# From hpclib
###
import linuxutils
from   urdecorators import trap
from   fileutils import *
from   sqlitedb import SQLiteDB
from   dorunrun import dorunrun, ExitCode
###
# imports and objects that are a part of this project
###
import os
import argparse
import random
import time
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

# For signal handler
db_handle = None
def handle(signum:int, stack:object=None) -> None:
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
        yield random.randiant(lower, upper)


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
def filter_datum(data:dict, key:str = None) -> pd.Dataframe:
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
    except Exception as e:
        return e
    
    return df

@trap
def merge_dfs(main_df: pd.DataFrame, *dfs: pd.DataFrame) -> pd.DataFrame:
    """
    This function merges multiple DataFrames into the main DataFrame using a left join on the index.

    :param main_df: The main DataFrame.
    :param dfs: Additional DataFrames to be merged.
    :return: The merged DataFrame.
    """
    try:
        for df in dfs:
            df = df.set_index(main_df.index)
            main_df = main_df.merge(df, left_index=True, right_index=True, how='left')
        return main_df
    except Exception as e:
        return e
     
@trap
def datacollector_main(myargs:argparse.Namespace) -> int:
    global db_handle
    # df_def     
    def_cmd = "/usr/sbin/cv-cockpit-helper --stat-definition"
    defs = collect_data(def_cmd)
    df_def = filter_data(defs, myargs.key)
    
    # df_value
    value_cmd = "sudo cv-stats -a --format=json"
    value = collect_data(value_cmd)
    df_value = filter_data(value)
    df_valdef = merge_dfs(df_value, df_def)    
    try:
        db_handle = db = SQLiteDB(myargs.db)
        # For TABLE FACTS
        sql_facts = """INSERT INTO FACTS(t, indexs, devices, datum)
                   VALUES (?, ?, ?, ?)"""
        facts_values= read_whitespace_file(myargs.file)
        db.executemany(sql_facts, facts_values)
    except Exception as e:
        print(e)
    finally:
        db.commit() 
    dither_iter = dither_time(myargs.freq_ 
    
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
    parser.add_argument('--key', type=str, default="", 
        help="Key to filter the data dictionary")
    parser.add_argument('--db', type=str, required=True, 
        help="Name of database")
    parser.add_argument('--file', type=str, required=True,
        help="Name of the file to filter interested cv_stats data")
    parser.add_argument('-f', '--freq', type=int, default=600,
        help='number of seconds between polls (default:600)')
    parser.add_argument('-n', type=int, default=sys.maxsize,
        help="For debugging, limit number of readings (default:unlimited)")
    myargs = parser.parse_args()
    verbose = myargs.verbose
    
    for _ in caught_signals:
         try:
             signal.signal(_, handler)
         except OSError as e:
             myargs.verbose and sys.stderr.write(f"Cannot reassign signal {_}\n")
         else:
             myargs.verbose and sys.stderr.write(f"Signal {_} is being handled.\n")

     exit_code = ExitCode(veryhungrycluster_main(myargs))
     print(exit_code)
     sys.exit(int(exit_code))

    try:
        outfile = sys.stdout if not myargs.output else open(myargs.output, 'w')
        with contextlib.redirect_stdout(outfile):
            sys.exit(globals()[f"{os.path.basename(__file__)[:-3]}_main"](myargs))

    except Exception as e:
        print(f"Escaped or re-raised exception: {e}")

