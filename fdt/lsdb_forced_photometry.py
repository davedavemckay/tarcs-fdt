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
from dask.distributed import Client
from datetime import datetime, timedelta

filter_names = ['u', 'g', 'r', 'i', 'z', 'y']
filter_colors = get_multiband_plot_colors()
filter_symbols = get_multiband_plot_symbols()

def download_data(fs, bucket, prefix):
    # List all files under the prefix
    files = fs.glob(f'{bucket}/{prefix}/**')
    
    # Download each file to a local directory
    local_dir = f'data/{bucket}/{prefix}'
    os.makedirs(local_dir, exist_ok=True)
    
    for f in files[1:]:  # skip the first entry which is the prefix itself
        local_path = os.path.join(local_dir, f.split(f'{bucket}/{prefix}/')[1])
        if not os.path.exists(local_path):
            fs.get(f, local_path)
            print(f'Downloaded {f} to {local_path}')
        else:
            print(f'{local_path} already exists, skipping download.')

def extract_objectForcedSource_light_curve(cat, objectId):
    # Implementation for extracting light curve for a specific object
    # Retrieve object information from the catalog using the objectId
    objectResult = cat.query(f"objectId == {objectId}")
    # Compute the objectForcedSource data for the retrieved object
    objectForcedSource = objectResult['objectForcedSource'].compute()
    # Extract the light curve data for the object
    objectLightCurve = objectForcedSource.iloc[0]
    return objectLightCurve

def visualise_light_curve(lightCurve, objectId):
    fig = plt.figure(figsize=(6, 4))
    for f, filt in enumerate(filter_names):
        tx = (lightCurve['band'] == filt)
        plt.plot(lightCurve['midpointMjdTai'][tx], lightCurve['psfMag'][tx],
                filter_symbols[filt], ms=5, mew=0, alpha=0.5, color=filter_colors[filt], label=filt)
    plt.ylim([20, 14])
    plt.legend(loc='lower left', ncol=3)
    plt.xlabel('MJD')
    plt.ylabel('PSF Magnitude')
    plt.title('Nested Forced Photometry Light Curve')
    plt.savefig(f'light_curve_{objectId}.png')

def main():
    parser = argparse.ArgumentParser(description='Run an LSDB Cone Search on Cirrus')
    parser.add_argument('--object-id', type=int, required=True, help='Object ID')
    parser.add_argument('--s3-endpoint-url', type=str, help='S3 endpoint URL for data access between Cirrus and Somerville')
    parser.add_argument('--s3-bucket', type=str, help='S3 bucket for data access between Cirrus and Somerville')
    parser.add_argument('--download-prefix', type=str, help='S3 prefix to download recursively and use as data for Cone Search')
    parser.add_argument('--upload-prefix', type=str, help='S3 prefix to upload results of Cone Search')
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

    catalog = lsdb.open_catalog(os.path.join('data', str(args.s3_bucket), str(args.download_prefix)))

    lightCurve = extract_objectForcedSource_light_curve(catalog, args.object_id)

    visualise_light_curve(lightCurve, args.object_id)

    data_end = datetime.now()

    print(f"Light curve produced in {(data_end - data_start).total_seconds():.2f} seconds.")

    tarcs_s3.put(f'light_curve_{args.object_id}.png', f"{args.s3_bucket}/{args.upload_prefix}/light_curve_{args.object_id}.png")

    print(f"Results uploaded in {(datetime.now() - data_end).total_seconds():.2f} seconds.")

    print(f"Total time: {(datetime.now() - all_start).total_seconds():.2f} seconds.")

if __name__ == "__main__":
    main()