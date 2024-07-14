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
from   dorunrun     import dorunrun, ExitCode
from   fileutils    import *
import linuxutils
from   linuxutils   import *
from   sloppytree   import SloppyTree
from   sqlitedb     import SQLiteDB
from   urdecorators import trap
from   urlogger     import URLogger
###
# imports and objects that are a part of this project
###
import random
import time
import signal
import pandas as pd
import json
import io

###
# Globals
###
actions = {'freq':{'lower': 1,
                   'upper': 1440,
                   'e_msg': 'Frequency cannot be accepted'}}

# How we get the data
value_cmd = "sudo cv-stats -a --format=json"

# These are the signals we are intercepting.
caught_signals = (  signal.SIGINT, signal.SIGQUIT,                     
                    signal.SIGUSR1, signal.SIGUSR2, 
                    signal.SIGTERM, signal.SIGHUP,
                    signal.SIGRTMIN+8 )
                    
# These are uninitalized globals
db_handle = None
favored_indices = None
filter_file_name = None
logger = None

# This is a case where we assume all will go well, at least
# at the beginning.
OK_to_continue = True 

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


@trap
def collect_datum(cmd:str) -> dict:
    """
    This function executes a Linux command through the dorunrun function and expects
    the output to be a nested dictionary from the standard output (stdout).

    :param cmd: The Linux command to be executed.
    :return: A dictionary containing the parsed JSON output from stdout. If an error occurs,
             a dictionary with the key 'Error' and the exception message is returned.
    """
    global logger

    try:
        result = SloppyTree(dorunrun(cmd, return_datatype=dict))
        if not result.OK:
            logger.error(f"{result.name=}")
            logger.error(f"{result.stderr=}")
            return json.loads({})

    except Exception as e:
        logger.error(f"{cmd=} generated {e=}")
        return json.loads({})

    return json.loads(result['stdout'])


@trap
def collect_indices(file_name:str) -> None:
    global favored_indices, logger

    favored_indices = tuple(_ for _ in read_whitespace_file(file_name) 
        if not _.startswith('#'))
    

@trap
def dither_time(t:int) -> int:
    """
    Avoid measuring the power at regular intervals
    """
    lower = int(t * 0.95)
    upper = int(t * 1.05)
    while True:
        yield random.randint(lower, upper)


@trap
def file_exists(value):
    if not os.path.isfile(value):
        raise argparse.ArgumentTypeError(f"File {value} does not exist.")
    return value


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
    global logger


    df = pd.DataFrame()

    try:
        df = pd.DataFrame(data[key]).T

    except KeyError:
        df = pd.DataFrame(data).T
        df.reset_index(inplace = True)
        df.rename(columns={df.columns[0]: 'indices'}, inplace=True)

    except Exception as e:
        logger.info(f"filter_datum {e=}")

    finally:
        return df


@trap
def handler(signum:int, stack:object=None) -> None:
    """
    Universal signal handler
    """
    global logger, db_handle, OK_to_continue
    logger.info(f"Received {signum=}")

    ###
    # Let's use signal 42, the meaning of life, for the child
    # to tell the parent there are problems. Note that the parent
    # does not stop running right away; instead, the main event loop
    # will not run next time it is invoked.
    ###
    if signum == signal.SIGRTMIN+8:
        OK_to_continue = False
        return
    
    ###
    # Traditionally, SIGHUP is used to re-read the config.
    ###
    elif signum == signal.SIGHUP:
        logger.info(f"Rereading configuration from {filter_file_name}")
        collect_indices(filter_file_name)
        return

    ###
    # Let's use SIGUSR1 to close the database, and exit gracefully.
    ###
    elif signum in (signal.SIGUSR1, signal.SIGTERM, signal.SIGQUIT): 
        try:
            db_handle.commit()
            db_handle.close()
        except Exception as e:
            logger.error(f"Error on exit {e=}\n")
            sys.exit(os.EX_IOERR)
        else:
            logger.info("Normal termination")
            sys.exit(os.EX_OK)

    ###
    # And SIGUSR2 will let take an additional measurement /now/. 
    # There are at least two uses: [1] We have noted something anomalous,
    # and want to preserve the data, or [2] We want to run the measurements
    # somewhat interactively as a form of testing.
    #
    # Note that we are not forking a separate process.
    ###
    elif signum == signal.SIGUSR2:
        logger.info("Taking a reading now.")
        take_a_reading()
        return 
      
    else:  
        logger.info(f"Received stray signal {signum}.")
        return


@trap
def populate_facts(df:pd.DataFrame, db:object) -> bool:
    """
    This function populates FACTS table for cluster built by ACT company
    The FACTs table includes cv_stats datum
    
    The purpose is to write pandas DataFrame to SQLite3
    :param df: the pandas DataFrame  
    :param db: the database connection    
    """

    OK = None
    try:
    
        # For TABLE FACTS
        sql_facts = """INSERT INTO FACTS(indices, devices, datum)
                   VALUES (?, ?, ?)"""
        facts_values = df[['indices','nodenames','datum']]
        return (OK := db.executemany_SQL(sql_facts, facts_values)) == -1
        
    except Exception as e:
        logger.error(f"{e=}")

    finally:
        logger.debug(f"executemany_SQL returned {OK}")


@trap
def take_a_reading() -> None:
    """
    This function allows us to invoke just one function in the child process.
    The encapsulation increases the flexibility we have in invoking it.
    """
    global value_cmd, db_handle, logger, favored_indices
    value = collect_datum(value_cmd)
    df = filter_datum(value)
    df = df[df['indices'].isin(favored_indices)]
    df = df.melt(id_vars=['indices'], var_name='nodenames', value_name='datum')

    if (error := populate_facts(df, db_handle)): 
        logger.warning(f"populate_facts() failed with {error=}.")
        tell_parent_to_stop()

    else:
        logger.debug("Finished populate_facts iteration")


@trap
def tell_parent_to_stop() -> None:
    """
    This is in a function so that we can remember what it does, and so that
    if we need to perform additional steps other than sending this one signal
    we can do it here.
    """
    os.kill(os.getppid(), signal.SIGRTMIN+8)


@trap
def collector_main(myargs:argparse.Namespace) -> int:

    # db connection
    global db_handle, value_cmd, logger
    global OK_to_continue, favored_indices, filter_file_name

    # going demonic, we need to get the FQN.
    myargs.db = os.path.realpath(myargs.db)
    logger.info(f"{myargs.db=}")
    db_handle = SQLiteDB(myargs.db)    
    logger.debug(f"{myargs.db} is open")
    logger.debug(f"{db_handle=}")
    filter_file_name = os.path.realpath(myargs.filter)

    collect_indices(filter_file_name) 
    logger.debug(f"{filter_file_name=}")
    logger.debug(f"{favored_indices=}")
    dither_iter = dither_time(myargs.freq * 60)

    # separate this program from the console/keyboard, and run it
    # in the background as a child of process 1.
    myargs.no_daemon or linuxutils.daemonize_me()
    
    error = 0 
    n=0
    
    while OK_to_continue and (n := n+1) < myargs.n:
        if (pid := os.fork()):
            time.sleep(next(dither_iter))
            continue

        try:
            take_a_reading()

        finally:
            os._exit(os.EX_OK)

    else:
        logger.info(f"while-condition is now False. {OK_to_continue=} {n=}")
    
    return os.EX_OK


if __name__ == '__main__':
    here       = os.getcwd()
    progname   = os.path.basename(__file__)[:-3]
    configfile = f"{here}/{progname}.toml"
    lookupkey  = "lookup"
    database   = f"{here}/clusterdata.db"
    logfile    = f"{here}/{progname}.log"
    lockfile   = f"{here}/{progname}.lock"

    parser = argparse.ArgumentParser(prog="collector", 
        description="What collector does, collector does best.")
    
    parser.add_argument('--db', type=file_exists, default=database, 
        help=f"Name of database, defaults to {database}")
    parser.add_argument('--filter', type=file_exists, required=True,
        help="Name of the file to filter interesting cv_stats data")
    parser.add_argument('--freq', type=int, choices=range(10,70,10), default=10,
        help='number of minutes between polls (default:10)')
    parser.add_argument('--key', type=str, default=lookupkey, 
        help=f"Key to filter the data dictionary, defaults to {lookupkey}")
    parser.add_argument('--loglevel', type=int, 
        choices=range(logging.FATAL, logging.NOTSET, -10), 
        default=logging.DEBUG, 
        help=f"Logging level, defaults to {logging.DEBUG}")
    parser.add_argument('--no-daemon', action='store_true', 
        help=f"Run in the foreground.")
    parser.add_argument('-o', '--output', type=str, default="",
        help="Output file name. Default is the terminal rather than a file.")
    parser.add_argument('-n', type=int, default=sys.maxsize,
        help=f"For debugging, limit number of readings. The default is unlimited.")
    parser.add_argument('-z', '--zap', action='store_true', 
        help=f"Remove {logfile} and create a new one.")

    myargs = parser.parse_args()
    logger = URLogger(logfile=logfile, level=myargs.loglevel)

    # Don't do anything until we are sure there is no other copy
    # of this daemon running.
    try:
        with linuxutils.LockFile(lockfile) as lock:
        
            if myargs.zap:
                try:
                    os.unlink(logfile)
                finally:
                    logger = URLogger(logfile=logfile, level=myargs.loglevel)
                

            
            # Not all signals can be reassigned. Let's go through the list and 
            # route as many as possible to SIG_IGN, the "ignore" block.
            for _ in range(1, signal.SIGRTMAX):
                try:
                    signal.signal(_, signal.SIG_IGN)
                    if _ in caught_signals:
                        signal.signal(_, handler)
                
                except OSError as e:
                    # Just note this in the logfile.
                    logger.info(f"signal {_} not reassigned.")

            # if we are running interactively, we want control-C to interrupt.
            myargs.no_daemon and signal.signal(signal.SIGINT, signal.SIG_DFL)

            try:
                # Let's try to do something useful.
                outfile = sys.stdout if not myargs.output else open(myargs.output, 'w')
                with contextlib.redirect_stdout(outfile):
                    exit_code = ExitCode(collector_main(myargs))
                    logger.info(f"{exit_code=}")
                    sys.exit(int(exit_code))

            except KeyboardInterrupt as e:
                logger.info(f"Control C pressed. Exiting.")
                sys.exit(os.EX_OK)

            except Exception as e:
                logger.error(f"Escaped or re-raised exception: {e}")


    except Exception as e:
        logger.error(f"{progname} is already running")


