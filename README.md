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

## InfluxDB to VictoriaMetrics Migration (migrate_influx2vm.py)

Migrate existing AskSinAnalyzer data from InfluxDB 2.x to VictoriaMetrics.

**Requirements:** `influxdb-client` Python package (`pip install influxdb-client`)

```
usage: migrate_influx2vm.py [-h] [--ifdb-config IFDB_CONFIG] [--vm-config VM_CONFIG]
                            [--start START_TIME] [--end END_TIME] [--all]
                            [--batch-size BATCH_SIZE] [--chunk-days CHUNK_DAYS]
                            [--dry-run] [--verbose]

Migrate AskSinAnalyzer data from InfluxDB 2.x to VictoriaMetrics

optional arguments:
  -h, --help            show this help message and exit
  --ifdb-config IFDB_CONFIG
                        Path to InfluxDB configuration file (default: ~/.asa-to-ifdb.conf)
  --vm-config VM_CONFIG
                        Path to VictoriaMetrics configuration file (default: ~/.asa-to-vm.conf)
  --start START_TIME    Start time for migration (e.g., 2024-01-01, -30d, -1y)
  --end END_TIME        End time for migration (e.g., 2024-12-31, now) (default: now)
  --all                 Migrate all data from InfluxDB (uses start time of 1970-01-01)
  --batch-size BATCH_SIZE
                        Number of data points per batch write (default: 10000)
  --chunk-days CHUNK_DAYS
                        Number of days per query chunk when using --all (default: 7)
  --dry-run, -t         Do not write any data - just show what would be migrated
  --verbose, -v         Enable verbose output
```

Examples:
```bash
# Dry-run migration of all data
migrate_influx2vm.py --all --dry-run

# Migrate all data
migrate_influx2vm.py --all

# Migrate specific time range
migrate_influx2vm.py --start 2024-01-01 --end 2024-12-31

# Migrate last 30 days
migrate_influx2vm.py --start=-30d
```

## Grafana Dashboards

Two Grafana dashboard JSON files are provided:
- `AskSin-Grafana-Dashboard.json` - Dashboard for InfluxDB data source
- `AskSin-Grafana-Dashboard-VM.json` - Dashboard for VictoriaMetrics/Prometheus data source

![Grafana Dashboard](https://github.com/bolausson/AskSinAnalyzer-CSV-to-InfluxDB/blob/main/AskSin-Grafana-Dashboard.png?raw=true)
