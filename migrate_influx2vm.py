#!/bin/env python3
# -*- coding: utf-8 -*-
#
# -----------------------------------------------------------------------------
# ifdb-to-vm.py, Copyright Bjoern Olausson
# -----------------------------------------------------------------------------
#
# This tool migrates data from InfluxDB 2.x to VictoriaMetrics.
# It reads the AskSinAnalyzer Telegrams data from InfluxDB and writes
# them to VictoriaMetrics in the same format as asa-to-vm.py.
#

import sys
import time
import json
import argparse
import configparser
import requests
from pathlib import Path
from datetime import datetime, timedelta
from os.path import expanduser
from influxdb_client import InfluxDBClient

default_ifdb_config = expanduser("~/.asa-to-ifdb.conf")
default_vm_config = expanduser("~/.asa-to-vm.conf")

parser = argparse.ArgumentParser(
    description='Migrate AskSinAnalyzer data from InfluxDB 2.x to VictoriaMetrics',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  %(prog)s --all --dry-run                    # Dry-run migration of all data
  %(prog)s --all                              # Migrate all data
  %(prog)s --start 2024-01-01 --end 2024-12-31
  %(prog)s --start 2024-01-01 --end 2024-12-31 --dry-run
  %(prog)s --start -30d --end now
  %(prog)s --ifdb-config /path/to/ifdb.conf --vm-config /path/to/vm.conf --all
"""
)

parser.add_argument('--ifdb-config', dest='ifdb_config', default=default_ifdb_config,
                    help=f'Path to InfluxDB configuration file (default: {default_ifdb_config})')
parser.add_argument('--vm-config', dest='vm_config', default=default_vm_config,
                    help=f'Path to VictoriaMetrics configuration file (default: {default_vm_config})')
parser.add_argument('--start', dest='start_time', default=None,
                    help='Start time for migration (e.g., 2024-01-01, -30d, -1y)')
parser.add_argument('--end', dest='end_time', default='now',
                    help='End time for migration (e.g., 2024-12-31, now) (default: now)')
parser.add_argument('--all', dest='migrate_all', action='store_true', default=False,
                    help='Migrate all data from InfluxDB (uses start time of 1970-01-01)')
parser.add_argument('--batch-size', dest='batch_size', type=int, default=10000,
                    help='Number of data points per batch write (default: 10000)')
parser.add_argument('--chunk-days', dest='chunk_days', type=int, default=7,
                    help='Number of days per query chunk when using --all (default: 7)')
parser.add_argument('--dry-run', '-t', dest='dryrun', action='store_true', default=False,
                    help='Do not write any data - just show what would be migrated')
parser.add_argument('--verbose', '-v', dest='verbose', action='store_true', default=False,
                    help='Enable verbose output')

args = parser.parse_args()

# Validate arguments
if not args.start_time and not args.migrate_all:
    print('Error: Either --start or --all must be specified', file=sys.stderr)
    parser.print_help()
    sys.exit(1)

if args.migrate_all:
    args.start_time = '1970-01-01'

# Read InfluxDB configuration
if not Path(args.ifdb_config).is_file():
    print(f'Error: InfluxDB configuration file not found: {args.ifdb_config}', file=sys.stderr)
    print(f'Create it using: asa-to-ifdb.py -cc -cf {args.ifdb_config}', file=sys.stderr)
    sys.exit(1)

ifdb_config = configparser.ConfigParser()
ifdb_config.read(args.ifdb_config)

IFDB_URL = ifdb_config['IFDB']['IFDB_URL']
IFDB_PORT = ifdb_config['IFDB']['IFDB_PORT']
IFDB_ORG = ifdb_config['IFDB']['IFDB_ORG']
IFDB_BUCKET = ifdb_config['IFDB']['IFDB_BUCKET']
IFDB_TOKEN = ifdb_config['IFDB']['IFDB_TOKEN']
IFDB_VERIFY_SSL = ifdb_config['IFDB']['IFDB_VERIFY_SSL'].lower() in ['true', '1']

# Read VictoriaMetrics configuration
if not Path(args.vm_config).is_file():
    print(f'Error: VictoriaMetrics configuration file not found: {args.vm_config}', file=sys.stderr)
    print(f'Create it using: asa-to-vm.py -cc -cf {args.vm_config}', file=sys.stderr)
    sys.exit(1)

vm_config = configparser.ConfigParser()
vm_config.read(args.vm_config)

VM_URL = vm_config['VM']['VM_URL']
VM_PORT = vm_config['VM']['VM_PORT']
VM_VERIFY_SSL = vm_config['VM']['VM_VERIFY_SSL'].lower() in ['true', '1']
VM_USERNAME = vm_config['VM'].get('VM_USERNAME', '')
VM_PASSWORD = vm_config['VM'].get('VM_PASSWORD', '')

VM_IMPORT_URL = f'{VM_URL}:{VM_PORT}/api/v1/import'
VM_AUTH = (VM_USERNAME, VM_PASSWORD) if VM_USERNAME and VM_PASSWORD else None

current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

def parse_time_arg(time_arg):
    """Parse time argument and return Flux-compatible time string"""
    if time_arg == 'now':
        return 'now()'
    # Check for relative time format like -30d, -1y
    if time_arg.startswith('-') and time_arg[-1] in 'dhms':
        return time_arg
    # Otherwise assume ISO date format
    try:
        dt = datetime.fromisoformat(time_arg)
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        print(f'Error: Invalid time format: {time_arg}', file=sys.stderr)
        print('Use ISO format (YYYY-MM-DD) or relative format (-30d, -1y)', file=sys.stderr)
        sys.exit(1)

start_time = parse_time_arg(args.start_time)
end_time = parse_time_arg(args.end_time)

print(f'Migration settings:')
print(f'  InfluxDB: {IFDB_URL}:{IFDB_PORT}, bucket: {IFDB_BUCKET}')
print(f'  VictoriaMetrics: {VM_URL}:{VM_PORT}')
print(f'  Time range: {start_time} to {end_time}')
print(f'  Batch size: {args.batch_size}')
print(f'  Dry run: {args.dryrun}')
print()

# Connect to InfluxDB
print('Connecting to InfluxDB...')
ifdbc = InfluxDBClient(
    url=f'{IFDB_URL}:{IFDB_PORT}',
    token=IFDB_TOKEN,
    org=IFDB_ORG,
    verify_ssl=IFDB_VERIFY_SSL,
    timeout=(6000, 6000)
)
ifdbc_read = ifdbc.query_api()

def format_vm_datapoint(metric_name, tags, value, timestamp_ms):
    """Format a single data point for VictoriaMetrics native JSON format"""
    metric = {"__name__": metric_name}
    metric.update(tags)
    return {
        "metric": metric,
        "values": [value],
        "timestamps": [timestamp_ms]
    }

def write_to_victoriametrics(datapoints):
    """Write datapoints to VictoriaMetrics"""
    if not datapoints:
        return True

    # Convert to newline-delimited JSON (NDJSON)
    data = '\n'.join(json.dumps(dp) for dp in datapoints)

    try:
        t0 = time.time()
        response = requests.post(
            VM_IMPORT_URL,
            data=data,
            headers={'Content-Type': 'application/json'},
            auth=VM_AUTH,
            verify=VM_VERIFY_SSL,
            timeout=600
        )
        response.raise_for_status()
        t1 = time.time()
        if args.verbose:
            print(f'  VictoriaMetrics write took {t1 - t0:.2f} seconds')
        return True
    except Exception as e:
        print(f'Error writing to VictoriaMetrics: {e}', file=sys.stderr)
        return False

def process_records(tables, vm_datapoints, total_records, total_datapoints, batches_written):
    """Process InfluxDB records and write to VictoriaMetrics"""
    for table in tables:
        for record in table.records:
            total_records += 1

            # Extract timestamp (convert to milliseconds)
            ts = record.get_time()
            timestamp_ms = int(ts.timestamp() * 1000)

            # Extract tags from InfluxDB record
            tags = {
                "flags": str(record.values.get("flags", "")),
                "type": str(record.values.get("type", "")),
                "fromAddr": str(record.values.get("fromAddr", "")),
                "toAddr": str(record.values.get("toAddr", "")),
                "fromName": str(record.values.get("fromName", "")),
                "toName": str(record.values.get("toName", "")),
                "fromSerial": str(record.values.get("fromSerial", "")),
                "toSerial": str(record.values.get("toSerial", "")),
                "toIsIp": str(record.values.get("toIsIp", "")),
                "fromIsIp": str(record.values.get("fromIsIp", "")),
            }

            # Extract fields and create VictoriaMetrics datapoints
            fields = {
                "rssi": record.values.get("rssi"),
                "len": record.values.get("len"),
                "cnt": record.values.get("cnt"),
                "dc": record.values.get("dc"),
            }

            for field_name, field_value in fields.items():
                if field_value is not None:
                    metric_name = f"Telegrams_{field_name}"
                    dp = format_vm_datapoint(metric_name, tags, field_value, timestamp_ms)
                    vm_datapoints.append(dp)
                    total_datapoints += 1

            # Write batch if we've accumulated enough datapoints
            if len(vm_datapoints) >= args.batch_size:
                if args.dryrun:
                    print(f'  [DRY RUN] Would write batch of {len(vm_datapoints)} datapoints')
                    if args.verbose and batches_written == 0:
                        print('  Sample datapoint:')
                        print(f'    {json.dumps(vm_datapoints[0], indent=2)}')
                else:
                    print(f'  Writing batch of {len(vm_datapoints)} datapoints...')
                    if not write_to_victoriametrics(vm_datapoints):
                        print('Error: Failed to write batch, aborting', file=sys.stderr)
                        sys.exit(1)
                batches_written += 1
                vm_datapoints.clear()

    return vm_datapoints, total_records, total_datapoints, batches_written

def generate_time_chunks(start_dt, end_dt, chunk_days):
    """Generate time chunks for querying InfluxDB"""
    chunks = []
    current = start_dt
    delta = timedelta(days=chunk_days)

    while current < end_dt:
        chunk_end = min(current + delta, end_dt)
        chunks.append((current, chunk_end))
        current = chunk_end

    return chunks

def parse_to_datetime(time_str):
    """Parse time string to datetime object"""
    if time_str == 'now()':
        return datetime.now()
    if time_str.startswith('-'):
        # Relative time like -30d
        unit = time_str[-1]
        value = int(time_str[1:-1])
        if unit == 'd':
            return datetime.now() - timedelta(days=value)
        elif unit == 'h':
            return datetime.now() - timedelta(hours=value)
        elif unit == 'm':
            return datetime.now() - timedelta(minutes=value)
        elif unit == 's':
            return datetime.now() - timedelta(seconds=value)
        elif unit == 'y':
            return datetime.now() - timedelta(days=value*365)
    # ISO format
    return datetime.fromisoformat(time_str.replace('Z', '+00:00').replace('T', ' ').split('+')[0])

# Initialize counters
vm_datapoints = []
total_records = 0
total_datapoints = 0
batches_written = 0

# Determine if we need to chunk the queries
use_chunking = args.migrate_all or (args.start_time and args.start_time.startswith('-') and
                                     int(args.start_time[1:-1]) > 30 if args.start_time[-1] == 'd' else False)

if use_chunking and args.migrate_all:
    # For --all, we need to first find the earliest data point using InfluxQL (much faster)
    print('Finding earliest data in InfluxDB...')

    # Extract database name from bucket (format: "dbname/retention")
    ifdb_database = IFDB_BUCKET.split('/')[0] if '/' in IFDB_BUCKET else IFDB_BUCKET

    influxql_query = 'SELECT * FROM "Telegrams" ORDER BY time ASC LIMIT 1'
    influxql_url = f'{IFDB_URL}:{IFDB_PORT}/query'

    if args.verbose:
        print(f'  Query: {influxql_query}')

    try:
        response = requests.get(
            influxql_url,
            params={'db': ifdb_database, 'q': influxql_query},
            headers={'Authorization': f'Token {IFDB_TOKEN}'},
            verify=IFDB_VERIFY_SSL,
            timeout=60
        )

        if response.status_code != 200:
            print(f'Error querying InfluxDB: {response.status_code} {response.text}', file=sys.stderr)
            sys.exit(1)

        data = response.json()
        if 'results' in data and data['results'] and 'series' in data['results'][0]:
            first_time_str = data['results'][0]['series'][0]['values'][0][0]
            first_time = datetime.fromisoformat(first_time_str.replace('Z', '+00:00'))
            print(f'  Earliest data found: {first_time}')
            start_dt = first_time.replace(tzinfo=None)
        else:
            print('  No data found in InfluxDB')
            sys.exit(0)
    except Exception as e:
        print(f'Error finding earliest data: {e}', file=sys.stderr)
        sys.exit(1)

    end_dt = datetime.now()

    # Generate time chunks
    chunks = generate_time_chunks(start_dt, end_dt, args.chunk_days)
    print(f'  Will process {len(chunks)} time chunks of {args.chunk_days} days each')
    print()

    for i, (chunk_start, chunk_end) in enumerate(chunks):
        chunk_start_str = chunk_start.strftime('%Y-%m-%dT%H:%M:%SZ')
        chunk_end_str = chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ')

        print(f'Processing chunk {i+1}/{len(chunks)}: {chunk_start_str} to {chunk_end_str}')

        flux_query = f'''
from(bucket: "{IFDB_BUCKET}")
  |> range(start: {chunk_start_str}, stop: {chunk_end_str})
  |> filter(fn: (r) => r["_measurement"] == "Telegrams")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''

        try:
            t0 = time.time()
            tables = ifdbc_read.query(flux_query)
            t1 = time.time()
            if args.verbose:
                print(f'  InfluxDB query took {t1 - t0:.2f} seconds')

            vm_datapoints, total_records, total_datapoints, batches_written = process_records(
                tables, vm_datapoints, total_records, total_datapoints, batches_written
            )
        except Exception as e:
            print(f'Error querying chunk: {e}', file=sys.stderr)
            continue

else:
    # Single query for smaller time ranges
    flux_query = f'''
from(bucket: "{IFDB_BUCKET}")
  |> range(start: {start_time}, stop: {end_time})
  |> filter(fn: (r) => r["_measurement"] == "Telegrams")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''

    print('Querying InfluxDB for Telegrams data...')
    if args.verbose:
        print(f'  Query: {flux_query.strip()}')
    print()

    try:
        t0 = time.time()
        tables = ifdbc_read.query(flux_query)
        t1 = time.time()
        print(f'InfluxDB query took {t1 - t0:.2f} seconds')

        print('Processing records...')
        vm_datapoints, total_records, total_datapoints, batches_written = process_records(
            tables, vm_datapoints, total_records, total_datapoints, batches_written
        )
    except Exception as e:
        print(f'Error querying InfluxDB: {e}', file=sys.stderr)
        sys.exit(1)

# Write remaining datapoints
if vm_datapoints:
    if args.dryrun:
        print(f'  [DRY RUN] Would write final batch of {len(vm_datapoints)} datapoints')
    else:
        print(f'  Writing final batch of {len(vm_datapoints)} datapoints...')
        if not write_to_victoriametrics(vm_datapoints):
            print('Error: Failed to write final batch', file=sys.stderr)
            sys.exit(1)
    batches_written += 1

print()
print('Migration complete!')
print(f'  Total records processed: {total_records}')
print(f'  Total datapoints created: {total_datapoints}')
print(f'  Batches written: {batches_written}')

if args.dryrun:
    print()
    print('[DRY RUN] No data was actually written to VictoriaMetrics')

ifdbc.close()

