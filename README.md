# Spydur Data Collection

## Description

This repository contains scripts and data files for collecting and analyzing cloud cluster usage across 32 devices.

### collector.py
The `collector.py` script is responsible for gathering usage data from all 32 devices in the cloud cluster. It collects various metrics and logs them into a database for further analysis. This script can be customized with various optional arguments to specify the database, file filters, polling frequency, logging levels, and more.

### data_dict.py
The `data_dict.py` script provides a data dictionary that includes but is not limited to the general description of the usage indices collected by `collector.py`. This script helps in understanding the different metrics and their significance in the context of cloud cluster usage.

### clusterdata.sql
The `clusterdata.sql` file contains the schema of the database used to store the collected usage data. It defines the structure of the database, including tables, fields, and relationships, ensuring the data is organized and accessible for analysis.


## Basic operation

### Source the shell file.
`source collector.sh`

This will allow you to type "collector" to run the program.

### Check the help

```bash
collector -h

What collector does, collector does best.

optional arguments:
  -h, --help            show this help message and exit
  --db DB               Name of database, defaults to /usr/local/sw/collector/clusterdata.db
  --file FILE           Name of the file to filter interested cv_stats data
  --freq MINUTES        number of minutes between polls (default:10)
  --key KEY             Key to filter the data dictionary, defaults to lookup
  --loglevel {50,40,30,20,10}
                        Logging level, defaults to 10
  --no-daemon           Run in the foreground.
  -o OUTPUT, --output OUTPUT
                        Output file name
  -n N                  For debugging, limit number of readings (sys.maxsize)
  -z, --zap             Remove /usr/local/sw/collector/collector.log and create a new one.

```
