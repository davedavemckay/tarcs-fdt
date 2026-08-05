# Script to run an LSDB Cone Search on Cirrus
import argparse
import requests
import s3fs
import os
import sys
import lsdb
import json
import matplotlib.pyplot as plt
from lsst.utils.plotting import (get_multiband_plot_colors,
                                 get_multiband_plot_symbols)
import astropy.units as u
from astropy.coordinates import SkyCoord
from dask.distributed import Client, wait, get_client
import numpy as np
from datetime import datetime, timedelta

filter_names = ['u', 'g', 'r', 'i', 'z', 'y']
filter_colors = get_multiband_plot_colors()
filter_symbols = get_multiband_plot_symbols()

def download_data(fs, bucket, prefix, recursive=True):
    # List all files under the prefix
    files = fs.glob(f'{bucket}/{prefix}/**')
    
    # Download each file to a local directory
    local_dir = f'data/{bucket}/{prefix}'
    os.makedirs(local_dir, exist_ok=True)
    
    for f in files[1:]:  # skip the first entry which is the prefix itself
        local_path = os.path.join(local_dir, f.split(f'{bucket}/{prefix}/')[1])
        if not os.path.exists(local_path):
            fs.get(f, local_path, recursive=recursive)
            print(f'Downloaded {f} to {local_path}')
        else:
            print(f'{local_path} already exists, skipping download.')

def get_lc(cat, _id):
    try:
        lc = cat.query(f'diaObjectId == {str(_id)}')['diaSource'].compute().iloc[0]
        if lc:
            return lc
    except Exception as e:
        return None

def get_random_object(cat, objectIdColumn, i):
    while True:
        choice = str(np.random.choice(cat[objectIdColumn].values))
        print(choice)
        lcf = get_client().submit(get_lc, cat, choice)
        wait(lcf)
        lc = lcf.result()
        print(choice, type(lc))
        if lc != None:
            break
    return choice, lc

def main():
    parser = argparse.ArgumentParser(description='Download a dia_collection from S3 and select data from it for analysis.')
    parser.add_argument('--s3-endpoint-url', type=str, help='S3 endpoint URL for data access between Cirrus and Somerville')
    parser.add_argument('--s3-bucket', type=str, help='S3 bucket for data access between Cirrus and Somerville')
    parser.add_argument('--download-prefix', type=str, help='S3 prefix to download recursively and use as data for Cone Search')
    parser.add_argument('--dask-cluster-ip', type=str, help='IP address of the Dask cluster scheduler for distributed processing')
    args = parser.parse_args()

    with open(os.path.expanduser('~/.aws/credentials'), 'r') as credf:
        creds = {
            l.split(' = ')[0].strip():l.split(' = ')[1].strip() for l in credf.readlines() if l.startswith('aws')
        }
    tarcs_s3 = s3fs.S3FileSystem(
        key=creds['aws_access_key_id'],
        secret=creds['aws_secret_access_key'],
        endpoint_url=args.s3_endpoint_url
    )

    all_start = datetime.now()

    download_data(tarcs_s3, args.s3_bucket, args.download_prefix)

    print(f"Data download completed in {(datetime.now() - all_start).total_seconds():.2f} seconds.")

    sched_path = f"{os.environ['WORK']}/dask_scheduler.json"

    if os.path.exists(sched_path):
        with open(sched_path, 'r') as dsf:
            scheduler_info = json.load(dsf)
            saved_scheduler_ip_and_port = scheduler_info.get('address', '').split('//')[1]
            if saved_scheduler_ip_and_port != args.dask_cluster_ip:
                print(f"Error: Dask cluster IP from arguments ({args.dask_cluster_ip}) does not match saved scheduler IP ({saved_scheduler_ip_and_port}).")
                sys.exit('Cluster IP mismatch.')

    client = Client(scheduler_file=sched_path)

    data_start = datetime.now()

    dia_object_cat = lsdb.open_catalog(os.path.join('data', str(args.s3_bucket), str(args.download_prefix)))

    random_object_futures = []
    num_ids = 1
    for i in range(num_ids):
        print(i)
        random_object_futures.append(client.submit(get_random_object, dia_object_cat, 'diaObjectId', i))
    wait(random_object_futures)
    light_curve_tuples = [ f.result() for f in random_object_futures ]

    known_diaObjectId = '611255759837069401'  # from lsdb tutorial notebook, known to be a supernova
    light_curve_tuples.insert(0, (known_diaObjectId, get_lc(dia_object_cat, known_diaObjectId)))

    lc_end = datetime.now()

    print(f"{num_ids} potential supernovae (random dia_objects) and 1 known supernova identified in {(lc_end - data_start).total_seconds():.2f} seconds.")

    for lc in light_curve_tuples:
        try:
            lc.values()[0].write_catalog(f'data/lc_{lc.keys()[0]}', overwrite=True)
        except RuntimeError as e:
            if 'empty' in str(e):
                print('Warning: partial information for this object.')
            else:
                print(f'Error: {e}')
        except Exception as e:
            print(f'Error: {e}')

    data_end = datetime.now()

    print(f"Light curve catalogs produced in {(data_end - lc_end).total_seconds():.2f} seconds.")

    print(f"Total time: {(datetime.now() - all_start).total_seconds():.2f} seconds.")

if __name__ == "__main__":
    main()