# Spydur Data Collection

# Basic operation

## Source the shell file.
`source collector.sh`

This will allow you to type "collector" to run the program.

## Check the help

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
