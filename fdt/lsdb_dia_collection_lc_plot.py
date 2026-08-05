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
from dask.distributed import Client, wait
from datetime import datetime, timedelta
import tqdm

filter_names = ['u', 'g', 'r', 'i', 'z', 'y']
filter_colors = get_multiband_plot_colors()
filter_symbols = get_multiband_plot_symbols()

def list_of_ints(arg):
    return list(map(int, arg.split(',')))

def download_data(fs, bucket, prefix):
    # List all files under the prefix
    files = fs.glob(f'{bucket}/{prefix}/**')
    
    # Download each file to a local directory
    local_dir = f'data/{bucket}/{prefix}'
    os.makedirs(local_dir, exist_ok=True)
    with tqdm.tqdm(total=len(files)-1, desc='Downloading data') as pbar:
        for f in files[1:]:  # skip the first entry which is the prefix itself
            local_path = os.path.join(local_dir, f.split(f'{bucket}/{prefix}/')[1])
            if not os.path.exists(local_path):
                fs.get(f, local_path)
                print(f'Downloaded {f} to {local_path}')
            else:
                print(f'{local_path} already exists, skipping download.')
            pbar.update(1)

def extract_SN_diaObjectSource_light_curves(dia_object_cat, diaObjectId):
    # Implementation for extracting light curve for a specific object
    # Retrieve object information from the catalog using the objectId
    sn = dia_object_cat.query(f"diaObjectId == {diaObjectId}")
    # Compute the objectForcedSource data for the retrieved object
    sn_ds = sn['diaSource']
    sn_ds_df = sn_ds.compute()
    sn_ds_lc = sn_ds_df.iloc[0]

    sn_fs = sn['diaObjectForcedSource']
    sn_fs_df = sn_fs.compute()
    sn_fs_lc = sn_fs_df.iloc[0]

    return sn_fs_lc, sn_ds_lc

def visualise_light_curve(sn_fs_lc, sn_ds_lc, diaObjectId):
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(6, 8))
    for f, filt in enumerate(filter_names):
        tx1 = (sn_fs_lc['band'] == filt)
        tx2 = (sn_ds_lc['band'] == filt)
        ax1.plot(sn_fs_lc['midpointMjdTai'][tx1]-60000, sn_fs_lc['psfDiffFlux'][tx1],
                filter_symbols[filt], ms=5, mew=0, alpha=0.5, color=filter_colors[filt], label=filt)
        ax2.plot(sn_ds_lc['midpointMjdTai'][tx2]-60000, sn_ds_lc['psfFlux'][tx2],
                filter_symbols[filt], ms=5, mew=0, alpha=0.5, color=filter_colors[filt], label=filt)
        ax3.plot(sn_ds_lc['midpointMjdTai'][tx2]-60000, sn_ds_lc['psfMag'][tx2],
                filter_symbols[filt], ms=5, mew=0, alpha=0.5, color=filter_colors[filt], label=filt)
        del tx1, tx2
    ax1.set_xlim([620, 660])
    ax2.set_xlim([620, 660])
    ax3.set_xlim([620, 660])
    ax1.set_ylim([-10000, 10000])
    ax2.set_ylim([-10000, 10000])
    ax3.set_ylim([23.7, 21.3])
    ax1.set_xlabel('MJD-60000')
    ax2.set_xlabel('MJD-60000')
    ax3.set_xlabel('MJD-60000')
    ax1.set_ylabel('forced PSF Diff Flux')
    ax2.set_ylabel('diaSource PSF Flux')
    ax3.set_ylabel('diaSource PSF Mag')
    ax2.legend(loc='upper right', ncol=2)
    plt.tight_layout()
    plt.savefig(f'light_curve_{diaObjectId}.png')

def parallel_upload_results(fs, bucket, upload_prefix, diaObjectId):
    local_path = f'light_curve_{diaObjectId}.png'
    if os.path.exists(local_path):
        fs.put(local_path, f"{bucket}/{upload_prefix}/light_curve_{diaObjectId}.png")
        print(f"Uploaded {local_path} to {bucket}/{upload_prefix}/light_curve_{diaObjectId}.png")
    else:
        print(f"Error: {local_path} does not exist, cannot upload.")

def main():
    parser = argparse.ArgumentParser(description='Run an LSDB Cone Search on Cirrus')
    parser.add_argument('--s3-endpoint-url', type=str, help='S3 endpoint URL for data access between Cirrus and Somerville')
    parser.add_argument('--s3-bucket', type=str, help='S3 bucket for data access between Cirrus and Somerville')
    parser.add_argument('--download-prefix', type=str, help='S3 prefix to download recursively and use as data for Cone Search')
    parser.add_argument('--upload-prefix', type=str, help='S3 prefix to upload results of Cone Search')
    parser.add_argument('--dask-cluster-ip', type=str, help='IP address of the Dask cluster scheduler for distributed processing')
    parser.add_argument('--dia-objectid', type=int, help='List of object IDs to search for')
    args = parser.parse_args()

    print(f"Namespace: s3-endpoint-url={args.s3_endpoint_url}, s3-bucket={args.s3_bucket}, download-prefix={args.download_prefix}, upload-prefix={args.upload_prefix}, dask-cluster-ip={args.dask_cluster_ip}, dia-objectid={args.dia_objectid}")

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

    light_curve_future = client.submit(extract_SN_diaObjectSource_light_curves, catalog, args.dia_objectid)

    wait([light_curve_future])

    light_curve_visualisation_future = client.submit(visualise_light_curve, light_curve_future.result()[0], light_curve_future.result()[1], args.dia_objectid)

    wait([light_curve_visualisation_future])

    data_end = datetime.now()

    print(f"Light curve produced in {(data_end - data_start).total_seconds():.2f} seconds.")

    upload_future = client.submit(parallel_upload_results, tarcs_s3, args.s3_bucket, args.upload_prefix, args.dia_objectid)
    wait([upload_future])

    print(f"Results uploaded in parallel in {(datetime.now() - data_end).total_seconds():.2f} seconds.")

    print(f"Total time: {(datetime.now() - all_start).total_seconds():.2f} seconds.")

if __name__ == "__main__":
    main()