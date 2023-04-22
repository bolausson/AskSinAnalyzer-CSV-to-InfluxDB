# AskSinAnalyzer-CSV-to-InfluxDB
Python script which will read CSV files from [AskSin Analyzer XS](https://github.com/psi-4ward/AskSinAnalyzerXS) containing Homematic IP Telegram information (like DutyCycle) to InfluxDB

```
usage: asa-to-ifdb.py [-h] [-cc] [-cf CONFIGFILE] [-d CSVDIR] [-f CSVFILES] [-t] [-l]

Write data from AskSin Analyzer XS CSV files into InfluxDB

optional arguments:
  -h, --help            show this help message and exit
  -cc, --create-config
                        Create configuration file containing the InfluxDB credentials and quit.
                         Name and path can be changed from default (${HOME}/.asa-to-ifdb.conf)
                         to something else with "-c"
  -cf CONFIGFILE, --config-file CONFIGFILE
                        InfluxDB credential file
  -d CSVDIR, --dir CSVDIR
                        Directory containing the CSV files - all TelegramsXS_*.csv files in this
                        dir will be processed (e.g. /opt/analyzer)
  -f CSVFILES, --files CSVFILES
                        Specify the path of a file to be processed.
                        This argument can be used multiple times to process multiple files
  -t, --test            Do not write any data to InfluxDB - just echo the data to stdout
  -l, --latest          Only read the most recent file (usefull for e.g. cronjobs)
```

Crontab example:
```
# Read AskSinAnalyzer CSV to InfluxDB
*/2 * * * * /usr/local/bin/asa-to-ifdb.py -d /opt/analyzer -l >> /dev/null
```

![Grafana Dashboard](https://github.com/bolausson/AskSinAnalyzer-CSV-to-InfluxDB/blob/main/AskSin-Grafana-Dashboard.png?raw=true)
