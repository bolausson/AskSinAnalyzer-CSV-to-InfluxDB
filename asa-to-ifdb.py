#!/bin/env python3
# -*- coding: utf-8 -*-
#
# -----------------------------------------------------------------------------
# asa-to-ifdb.py, Copyright Bjoern Olausson
# -----------------------------------------------------------------------------
#
# This tool is inteded to read CSV files containing data gathered by
# AskSin Analyzer XS and write them as time series into InfluxDB
# 
# To delete all the date in the AskSinAnalyzer bucket (takes a while):
# influx config pi-https
# influx config list
# influx delete --bucket AskSinAnalyzer --start '2000-01-01T00:00:00Z' --stop $(date +"%Y-%m-%dT%H:%M:%SZ")
#

import os
import csv
import sys
import time
import argparse
import configparser
import codecs
from glob import glob
from pathlib import Path
from pprint import pprint
from datetime import datetime
from os.path import expanduser
from influxdb_client import Point, InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

default_config_file = expanduser("~/.asa-to-ifdb.conf")

parser = argparse.ArgumentParser(description='Write data from AskSin Analyzer XS CSV files into InfluxDB')

parser.add_argument('-cc', '--create-config', dest='createconfigfile', action='store_true', default=False,
                    help=f'Create configuration file containing the InfluxDB credentials and quit. Name and path can be changed from default ({default_config_file}) to something else with "-c"')
parser.add_argument('-cf', '--config-file', dest='configfile', default=default_config_file,
                    help='InfluxDB credential file')

parser_group_1 = parser.add_argument_group('Options')
parser.add_argument('-d', '--dir', dest='csvdir', default="", type=str,
                    help='Directory containing the CSV files - all TelegramsXS_*.csv files in this dir will be processed (e.g. /opt/analyzer)')
parser.add_argument('-f', '--files', dest='csvfiles', default=[], type=str, action='append',
                    help='Specify the path of a file to be processed. This argument can be used multiple times to process multiple files')
parser.add_argument('-t', '--test', dest='dryrun', action='store_true', default=False,
                    help='Do not write any data to InfluxDB - just echo the data to stdout')
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

if not CSVDIR and not CSVFILES:
    print("Either -d or -f must be specified!")
    print("Use -h for help")
    sys.exit()

if Path(CONFIGFILE).is_file() and CREATECONFIGFILE:
    print(f'Configuration file "{CONFIGFILE}" already exists!')
    print(f'Use a different name/path (with -cf) or delete "{CONFIGFILE}" to create a new default config')
    sys.exit()
elif not Path(CONFIGFILE).is_file() and CREATECONFIGFILE:
    config['IFDB'] = {
        'IFDB_URL': "Your-URL-to-InfluxDB 2.x Goes Here e.g. https://localhost",
        'IFDB_PORT': 8086,
        'IFDB_ORG': "Your-Org-Goes-Here",
        'IFDB_BUCKET': "AskSinAnalyzer",
        'IFDB_TOKEN': "Your-Token-Goes-Here",
        'IFDB_VERIFY_SSL': False,
    }

    with open(CONFIGFILE, 'w') as f:
        config.write(f)
        print(f'Configuration file {CONFIGFILE} created!')
        sys.exit()
else:
    config.read(CONFIGFILE)

IFDB_URL = config['IFDB']['IFDB_URL']
IFDB_PORT = config['IFDB']['IFDB_PORT']
IFDB_ORG = config['IFDB']['IFDB_ORG']
IFDB_BUCKET = config['IFDB']['IFDB_BUCKET']
IFDB_TOKEN = config['IFDB']['IFDB_TOKEN']

if config['IFDB']['IFDB_VERIFY_SSL'].lower() in ['true', '1']:
     IFDB_VERIFY_SSL = True
else:
     IFDB_VERIFY_SSL = False

current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

ifdbc = InfluxDBClient(url=f'{IFDB_URL}:{IFDB_PORT}', token=IFDB_TOKEN, org=IFDB_ORG, verify_ssl=IFDB_VERIFY_SSL, timeout=(6000,6000))
ifdbc_write = ifdbc.write_api(write_options=SYNCHRONOUS)
ifdbc_read = ifdbc.query_api()

CSVFILELIST = []
FIELDNAMES = ['tstamp', 'date', 'rssi', 'len', 'cnt', 'dc', 'flags', 'type', 'fromAddr', 'toAddr', 'fromName', 'toName', 'fromSerial', 'toSerial', 'toIsIp', 'fromIsIp', 'payload', 'raw']

# Get last data point from InfluxDB
#if_q = f'from(bucket: "{IFDB_BUCKET}") \
#|> range(start: 1970-01-01T00:00:00Z) \
#|> filter(fn: (r) => r["_measurement"] == "Telegrams") \
#|> filter(fn: (r) => r["_field"] == "date") \
#|> group() \
#|> last()'
#last_dp_date = ifdbc_read.query(if_q)

#last_dp_date_dict = {}
#for table in last_dp_date:
#    key = table.records[0].values["_field"]
#    value = table.records[0].values["_value"]
#    last_dp_date_dict[key] = value
#
#pprint(last_dp_date_dict)

if_q = f'from(bucket: "{IFDB_BUCKET}") \
|> range(start: 1970-01-01T00:00:00Z) \
|> filter(fn: (r) => r["_measurement"] == "Telegrams") \
|> filter(fn: (r) => r["_field"] == "tstamp") \
|> group() \
|> last()'

attempts = 3

for attempt in range(attempts):
    try:
        last_dp_ts = ifdbc_read.query(if_q)
    except Exception as e:
        #print(f'{current_time}', file=sys.stderr)
        #print(f'Failed to query the last data point from InfluxDB! (Try {attempt}/{attempts})', file=sys.stderr)
        #print("-----------------------------------------------------", file=sys.stderr)
        #print(e, file=sys.stderr)
        #print("+++++++++++++++++++++++++++++++++++++++++++++++++++++", file=sys.stderr)
        #pprint(e)
        #print("-----------------------------------------------------", file=sys.stderr)
        time.sleep(10)
    else:
        #if attempt > 0:
            #print(f'Try {attempt}/{attempts} was successfull!', file=sys.stderr)
        break
else:
    # The for "else" block will NOT be executed if the loop is stopped by a break statement.
    print(f'{current_time}', file=sys.stderr)
    print(f'All {attempts} attempts failed to query last datapoint from InfluxDB - giving up', file=sys.stderr)
    sys.exit()

#last_dp_ts_dict = {}
#for table in last_dp_ts:
#    key = table.records[0].values["_field"]
#    value = table.records[0].values["_value"]
#    last_dp_ts_dict[key] = value

#pprint(last_dp_ts_dict)

try:
    last_ts = last_dp_ts[0].records[0].values["_time"]
    last_ts_value = last_dp_ts[0].records[0].values["_value"]
except IndexError as e:
    pass

#print(last_ts)
#print(last_ts_value)

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

for file in SORTEDCSVFILELIST:
    print(f'Reading file {file} ({COUNTER}/{NUMFILES})')
    COUNTER += 1
    MEASUREMENT = []    

    with open(file) as f:
    #with codecs.open(file, 'rU', 'utf-8') as f:
        #fixed = [x.replace('\0', '0') for x in f]
        try:
            reader = csv.DictReader(f, delimiter=';', fieldnames=FIELDNAMES)
        except Exception as e:
            print(f'{current_time}', file=sys.stderr)
            print("-----------------------------------------------------", file=sys.stderr)
            print(f'File {file} could not be read by csv.DictReader! Skipping it!', file=sys.stderr)
            print(e, file=sys.stderr)
            #print("+++++++++++++++++++++++++++++++++++++++++++++++++++++", file=sys.stderr)
            #pprint(e)
            print("-----------------------------------------------------", file=sys.stderr)

        else:
            rowcounter = 1
            try:
                for row in reader:
                    rowcounter += 1
                    # Do not process the header line!
                    if row["tstamp"] != "tstamp":
                        #pprint(row)
                        #time.sleep(10)
                        ts = int(row["tstamp"])
                        #logtimestr = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%dT%H:%M:%S:{}Z').format(ts%1000)
                        MM = [
                            {
                                "measurement": 'Telegrams',
                                "tags": {
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
#                                    "payload": str(row["payload"]),
#                                    "raw": str(row["raw"]),
                                    },
                                "fields": {
                                    "tstamp": int(row["tstamp"]),
                                    "date": str(row["date"]),
                                    "rssi": int(row["rssi"]),
                                    "len": int(row["len"]),
                                    "cnt": int(row["cnt"]),
                                    "dc": float(row["dc"]),
                                    },
                                "time": int(row["tstamp"]),
                            },
                        ]

                        if LATEST and ts <= last_ts_value:
                            pass
                        else:
                            MEASUREMENT.extend(MM)
            except (csv.Error) as e:
                print(f'{current_time}', file=sys.stderr)
                print("-----------------------------------------------------", file=sys.stderr)
                print(f'File {file} is broken! Skipping it!', file=sys.stderr)
                print(f'Row: {rowcounter}', file=sys.stderr)
                print(e, file=sys.stderr)
                #print("+++++++++++++++++++++++++++++++++++++++++++++++++++++", file=sys.stderr)
                #pprint(e)
                print("-----------------------------------------------------", file=sys.stderr)

    if DRYRUN:
        #pass
        pprint(MEASUREMENT)
    else:
        #pass
        #pprint(MEASUREMENT)
        if len(MEASUREMENT):
            print("Writing data to InfluxDB")
        #print(f'ifdbc_write.write(bucket={IFDB_BUCKET}, org={IFDB_ORG}, write_precision="ms", record=MEASUREMENT)')
            try:
                ifdbc_write.write(bucket=IFDB_BUCKET, org=IFDB_ORG, write_precision='ms', record=MEASUREMENT)
            except Exception as e:
                print(f'{current_time}', file=sys.stderr)
                print("-----------------------------------------------------", file=sys.stderr)
                print(f'Failed to write to InfluxDB', file=sys.stderr)
                print(e, file=sys.stderr)
                #print("+++++++++++++++++++++++++++++++++++++++++++++++++++++", file=sys.stderr)
                #pprint(e)
                print("-----------------------------------------------------", file=sys.stderr)
                sys.exit()
        else:
            print("InfluxDB is up to date, there is no new data to write")
