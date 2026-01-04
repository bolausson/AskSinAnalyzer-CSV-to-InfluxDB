#!/bin/env python3
# -*- coding: utf-8 -*-
#
# -----------------------------------------------------------------------------
# asa-to-vm.py, Copyright Bjoern Olausson
# -----------------------------------------------------------------------------
#
# This tool is inteded to read CSV files containing data gathered by
# AskSin Analyzer XS and write them as time series into VictoriaMetrics
#

import os
import csv
import sys
import time
import json
import argparse
import configparser
import requests
from glob import glob
from pathlib import Path
from pprint import pprint
from datetime import datetime
from os.path import expanduser

default_config_file = expanduser("~/.asa-to-vm.conf")

parser = argparse.ArgumentParser(description='Write data from AskSin Analyzer XS CSV files into VictoriaMetrics')

parser.add_argument('-cc', '--create-config', dest='createconfigfile', action='store_true', default=False,
                    help=f'Create configuration file containing the VictoriaMetrics credentials and quit. Name and path can be changed from default ({default_config_file}) to something else with "-c"')
parser.add_argument('-cf', '--config-file', dest='configfile', default=default_config_file,
                    help='VictoriaMetrics credential file')

parser_group_1 = parser.add_argument_group('Options')
parser.add_argument('-d', '--dir', dest='csvdir', default="", type=str,
                    help='Directory containing the CSV files - all TelegramsXS_*.csv files in this dir will be processed (e.g. /opt/analyzer)')
parser.add_argument('-f', '--files', dest='csvfiles', default=[], type=str, action='append',
                    help='Specify the path of a file to be processed. This argument can be used multiple times to process multiple files')
parser.add_argument('-t', '--test', dest='dryrun', action='store_true', default=False,
                    help='Do not write any data to VictoriaMetrics - just echo the data to stdout')
parser.add_argument('-l', '--latest', dest='latest', action='store_true', default=False,
                    help='Only read the most recent file (usefull for e.g. cronjobs)')

args = parser.parse_args()
config = configparser.ConfigParser()

CREATECONFIGFILE = args.createconfigfile
CONFIGFILE = args.configfile
CSVDIR = args.csvdir
CSVFILES = args.csvfiles
DRYRUN = args.dryrun
LATEST = args.latest

if Path(CONFIGFILE).is_file() and CREATECONFIGFILE:
    print(f'Configuration file "{CONFIGFILE}" already exists!')
    print(f'Use a different name/path (with -cf) or delete "{CONFIGFILE}" to create a new default config')
    sys.exit()
elif not Path(CONFIGFILE).is_file() and CREATECONFIGFILE:
    config['VM'] = {
        'VM_URL': "Your-URL-to-VictoriaMetrics Goes Here e.g. http://localhost",
        'VM_PORT': 8428,
        'VM_VERIFY_SSL': False,
        'VM_USERNAME': "Optional: Your-Username-Goes-Here (leave empty if not using auth)",
        'VM_PASSWORD': "Optional: Your-Password-Goes-Here (leave empty if not using auth)",
        'VM_RANGE_START': "Start looking back for last result e.g. 30d or 365d",
    }

    with open(CONFIGFILE, 'w') as f:
        config.write(f)
        print(f'Configuration file {CONFIGFILE} created!')
        sys.exit()
else:
    config.read(CONFIGFILE)

if not CSVDIR and not CSVFILES:
    print("Either -d or -f must be specified!")
    print("Use -h for help")
    sys.exit()

VM_URL = config['VM']['VM_URL']
VM_PORT = config['VM']['VM_PORT']
VM_RANGE_START = config['VM'].get('VM_RANGE_START', '30d')
VM_USERNAME = config['VM'].get('VM_USERNAME', '')
VM_PASSWORD = config['VM'].get('VM_PASSWORD', '')

if config['VM']['VM_VERIFY_SSL'].lower() in ['true', '1']:
    VM_VERIFY_SSL = True
else:
    VM_VERIFY_SSL = False

current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

# VictoriaMetrics endpoints
VM_IMPORT_URL = f'{VM_URL}:{VM_PORT}/api/v1/import'
VM_QUERY_URL = f'{VM_URL}:{VM_PORT}/api/v1/query'

# Setup authentication if configured
VM_AUTH = None
if VM_USERNAME and VM_PASSWORD:
    VM_AUTH = (VM_USERNAME, VM_PASSWORD)

# Query VictoriaMetrics for the last timestamp
last_ts_value = 0

def get_last_timestamp():
    """Query VictoriaMetrics for the last processed timestamp"""
    global last_ts_value

    # Query for the maximum timestamp of any Telegrams metric
    # Using last_over_time to get the most recent data point
    query = f'last_over_time(Telegrams_cnt[{VM_RANGE_START}])'

    attempts = 3
    for attempt in range(attempts):
        try:
            t0 = time.time()
            response = requests.get(
                VM_QUERY_URL,
                params={'query': query},
                auth=VM_AUTH,
                verify=VM_VERIFY_SSL,
                timeout=60
            )
            response.raise_for_status()
            t1 = time.time()
            totalt = t1 - t0
            print(f'VictoriaMetrics query took {totalt:.2f} seconds')

            data = response.json()
            if data.get('status') == 'success' and data.get('data', {}).get('result'):
                # Find the maximum timestamp from all results
                # Timestamp is in seconds (float), convert to milliseconds
                max_ts = 0
                for result in data['data']['result']:
                    ts_seconds = float(result['value'][0])
                    ts_ms = int(ts_seconds * 1000)
                    if ts_ms > max_ts:
                        max_ts = ts_ms
                last_ts_value = max_ts
                print(f'Last timestamp in VictoriaMetrics: {last_ts_value}')
            else:
                print('No existing data found in VictoriaMetrics')
            return

        except requests.exceptions.ConnectionError as e:
            print(f'Warning: Could not connect to VictoriaMetrics (attempt {attempt + 1}/{attempts}): {e}', file=sys.stderr)
            time.sleep(10)
        except Exception as e:
            print(f'Warning: Error querying VictoriaMetrics (attempt {attempt + 1}/{attempts}): {e}', file=sys.stderr)
            time.sleep(10)

    print(f'All {attempts} attempts to query VictoriaMetrics failed - starting from scratch', file=sys.stderr)

# Only query for last timestamp if using -l/--latest flag
if LATEST:
    get_last_timestamp()

def format_vm_json_datapoint(measurement, tags, fields, timestamp_ms):
    """Format a data point as VictoriaMetrics native JSON format

    VictoriaMetrics JSON format:
    {
        "metric": {"__name__": "metric_name", "label1": "value1", ...},
        "values": [value1, value2, ...],
        "timestamps": [ts1_ms, ts2_ms, ...]
    }
    """
    datapoints = []
    for field_name, field_value in fields.items():
        if field_name in ['tstamp', 'date']:  # Skip non-numeric fields
            continue
        # Create metric with __name__ and all tags as labels
        metric = {
            "__name__": f"{measurement}_{field_name}"
        }
        # Add all tags as labels
        metric.update(tags)

        # Create datapoint
        datapoint = {
            "metric": metric,
            "values": [field_value],
            "timestamps": [timestamp_ms]
        }
        datapoints.append(datapoint)
    return datapoints

CSVFILELIST = []
FIELDNAMES = ['tstamp', 'date', 'rssi', 'len', 'cnt', 'dc', 'flags', 'type', 'fromAddr', 'toAddr', 'fromName', 'toName', 'fromSerial', 'toSerial', 'toIsIp', 'fromIsIp', 'payload', 'raw']

if CSVDIR:
    print("Globbing files to be processed")
    CSVFILESINDIR = glob(f"{CSVDIR}/TelegramsXS_*.csv")
    CSVFILELIST.extend(CSVFILESINDIR)

if len(CSVFILES) > 0:
    print("Files to be processed")
    CSVFILELIST.extend(CSVFILES)

SORTEDCSVFILELIST = sorted(CSVFILELIST, key=os.path.getmtime)

if LATEST:
    SORTEDCSVFILELIST = [SORTEDCSVFILELIST[-1]]
    print(f'Only using the latest file: {SORTEDCSVFILELIST[0]}')

NUMFILES = len(SORTEDCSVFILELIST)
COUNTER = 1
max_ts_processed = last_ts_value

for file in SORTEDCSVFILELIST:
    print(f'Reading file {file} ({COUNTER}/{NUMFILES})')
    COUNTER += 1
    VM_DATAPOINTS = []

    with open(file) as f:
        try:
            reader = csv.DictReader(f, delimiter=';', fieldnames=FIELDNAMES)
        except Exception as e:
            print(f'{current_time}', file=sys.stderr)
            print("-----------------------------------------------------", file=sys.stderr)
            print(f'File {file} could not be read by csv.DictReader! Skipping it!', file=sys.stderr)
            print(e, file=sys.stderr)
            print("-----------------------------------------------------", file=sys.stderr)

        else:
            rowcounter = 1
            try:
                for row in reader:
                    rowcounter += 1
                    # Do not process the header line!
                    if row["tstamp"] != "tstamp":
                        try:
                            ts = int(row["tstamp"])

                            tags = {
                                "flags": str(row["flags"]),
                                "type": str(row["type"]),
                                "fromAddr": str(row["fromAddr"]),
                                "toAddr": str(row["toAddr"]),
                                "fromName": str(row["fromName"]),
                                "toName": str(row["toName"]),
                                "fromSerial": str(row["fromSerial"]),
                                "toSerial": str(row["toSerial"]),
                                "toIsIp": str(row["toIsIp"]),
                                "fromIsIp": str(row["fromIsIp"]),
                            }

                            fields = {
                                "rssi": int(row["rssi"]),
                                "len": int(row["len"]),
                                "cnt": int(row["cnt"]),
                                "dc": float(row["dc"]),
                            }

                            if LATEST and ts <= last_ts_value:
                                pass
                            else:
                                datapoints = format_vm_json_datapoint("Telegrams", tags, fields, ts)
                                VM_DATAPOINTS.extend(datapoints)
                                if ts > max_ts_processed:
                                    max_ts_processed = ts
                        except (ValueError, KeyError) as e:
                            # Skip rows with invalid data (e.g., null bytes from corrupted files)
                            print(f'Warning: Skipping invalid row {rowcounter} in {file}: {e}', file=sys.stderr)
                            continue
            except (csv.Error) as e:
                print(f'{current_time}', file=sys.stderr)
                print("-----------------------------------------------------", file=sys.stderr)
                print(f'File {file} is broken! Skipping it!', file=sys.stderr)
                print(f'Row: {rowcounter}', file=sys.stderr)
                print(e, file=sys.stderr)
                print("-----------------------------------------------------", file=sys.stderr)

    if DRYRUN:
        # Print first few datapoints as sample
        sample_count = min(5, len(VM_DATAPOINTS))
        for i in range(sample_count):
            print(json.dumps(VM_DATAPOINTS[i], indent=2))
        if len(VM_DATAPOINTS) > sample_count:
            print(f'... and {len(VM_DATAPOINTS) - sample_count} more datapoints')
    else:
        if len(VM_DATAPOINTS):
            print(f"Writing {len(VM_DATAPOINTS)} data points to VictoriaMetrics")
            # Convert to newline-delimited JSON (NDJSON) - each datapoint on its own line
            # VictoriaMetrics /api/v1/import expects this format, not a JSON array
            data = '\n'.join(json.dumps(dp) for dp in VM_DATAPOINTS)

            try:
                t0 = time.time()
                auth = None
                if VM_USERNAME and VM_PASSWORD:
                    auth = (VM_USERNAME, VM_PASSWORD)

                response = requests.post(
                    VM_IMPORT_URL,
                    data=data,
                    headers={'Content-Type': 'application/json'},
                    auth=auth,
                    verify=VM_VERIFY_SSL,
                    timeout=600
                )
                response.raise_for_status()
                t1 = time.time()
                totalt = t1 - t0
                print(f'VictoriaMetrics write took {totalt:.2f} seconds')

            except Exception as e:
                t1 = time.time()
                totalt = t1 - t0
                print(f'{current_time}', file=sys.stderr)
                print(f'VictoriaMetrics write took {totalt:.2f} seconds before failing!', file=sys.stderr)
                print("-----------------------------------------------------", file=sys.stderr)
                print(f'Failed to write to VictoriaMetrics', file=sys.stderr)
                print(e, file=sys.stderr)
                print("-----------------------------------------------------", file=sys.stderr)
                sys.exit()
        else:
            print("VictoriaMetrics is up to date, there is no new data to write")

