# AskSinAnalyzer-CSV-to-InfluxDB
Python scripts to read CSV files from [AskSin Analyzer XS](https://github.com/psi-4ward/AskSinAnalyzerXS) containing Homematic IP Telegram information (like DutyCycle) and import them into InfluxDB or VictoriaMetrics.

## InfluxDB Import (asa-to-ifdb.py)

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

## VictoriaMetrics Import (asa-to-vm.py)

```
usage: asa-to-vm.py [-h] [-cc] [-cf CONFIGFILE] [-d CSVDIR] [-f CSVFILES] [-t] [-l]

Write data from AskSin Analyzer XS CSV files into VictoriaMetrics

optional arguments:
  -h, --help            show this help message and exit
  -cc, --create-config
                        Create configuration file containing the VictoriaMetrics settings and quit.
                         Name and path can be changed from default (${HOME}/.asa-to-vm.conf)
                         to something else with "-cf"
  -cf CONFIGFILE, --config-file CONFIGFILE
                        VictoriaMetrics configuration file
  -d CSVDIR, --dir CSVDIR
                        Directory containing the CSV files - all TelegramsXS_*.csv files in this
                        dir will be processed (e.g. /opt/analyzer)
  -f CSVFILES, --files CSVFILES
                        Specify the path of a file to be processed.
                        This argument can be used multiple times to process multiple files
  -t, --test            Do not write any data to VictoriaMetrics - just echo the data to stdout
  -l, --latest          Only read the most recent file (useful for e.g. cronjobs)
```

Crontab example:
```
# Read AskSinAnalyzer CSV to VictoriaMetrics
*/2 * * * * /usr/local/bin/asa-to-vm.py -d /opt/analyzer -l >> /dev/null
```

## Grafana Dashboards

Two Grafana dashboard JSON files are provided:
- `AskSin-Grafana-Dashboard.json` - Dashboard for InfluxDB data source
- `AskSin-Grafana-Dashboard-VM.json` - Dashboard for VictoriaMetrics/Prometheus data source

![Grafana Dashboard](https://github.com/bolausson/AskSinAnalyzer-CSV-to-InfluxDB/blob/main/AskSin-Grafana-Dashboard.png?raw=true)
