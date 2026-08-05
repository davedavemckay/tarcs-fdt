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

def visualise_cone_search(results_cat,ra,dec,radius):
    plt.figure(figsize=(7, 5))
    plt.hist2d(results_cat['coord_ra'], results_cat['coord_dec'],
            bins=200, cmap='viridis')
    plt.colorbar(label="Number of Objects per Bin")
    plt.xlabel("Right Ascension [deg]")
    plt.ylabel("Declination [deg]")
    plt.title(f"Cone Search Results:\nRA={ra} deg, Dec={dec} deg,\nRadius={radius} arcsec,\n({len(results_cat['coord_ra'])*len(results_cat['coord_dec'])} pixels in 200 bins)")
    plt.savefig(f'cone_search_results-{ra}_{dec}_{radius}.png')

def main():
    parser = argparse.ArgumentParser(description='Run an LSDB Cone Search on Cirrus')
    parser.add_argument('--ra', type=float, required=True, help='Right Ascension in degrees')
    parser.add_argument('--dec', type=float, required=True, help='Declination in degrees')
    parser.add_argument('--radius', type=float, required=True, help='Search radius in arc seconds')
    parser.add_argument('--s3-endpoint-url', type=str, help='S3 endpoint URL for data access between Cirrus and Somerville')
    parser.add_argument('--s3-bucket', type=str, help='S3 bucket for data access between Cirrus and Somerville')
    parser.add_argument('--download-prefix', type=str, help='S3 prefix to download recursively and use as data for Cone Search')
    parser.add_argument('--upload-prefix', type=str, help='S3 prefix to upload results of Cone Search')
    parser.add_argument('--columns', nargs='+', help='Columns to include in the catalog')
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

    catalog = lsdb.open_catalog(os.path.join('data', str(args.s3_bucket), str(args.download_prefix)), columns=['objectId'].extend(args.columns))

    results_cat = catalog.cone_search(args.ra, args.dec, args.radius)

    visualise_cone_search(results_cat, args.ra, args.dec, args.radius)

    results_cat.write_catalog(f'cone_search_results-{args.ra}_{args.dec}_{args.radius}', overwrite=True)

    # print(results_cat['objectId'].compute().values[:100])
    ind = results_cat.compute().index
    print(f'Objects found: {len(ind)}')
    if len(ind) > 100:
        print(ind[:100])
    else:
        print(ind)

    print(f"Cone search completed in {(datetime.now() - data_start).total_seconds():.2f} seconds.")

    tarcs_s3.put(f'cone_search_results-{args.ra}_{args.dec}_{args.radius}', f"{args.s3_bucket}/{args.upload_prefix}/cone_search_results-{args.ra}_{args.dec}_{args.radius}", recursive=True)
    tarcs_s3.put(f'cone_search_results-{args.ra}_{args.dec}_{args.radius}.png', f"{args.s3_bucket}/{args.upload_prefix}/cone_search_results-{args.ra}_{args.dec}_{args.radius}.png")

    print(f"Total time: {(datetime.now() - all_start).total_seconds():.2f} seconds.")

if __name__ == "__main__":
    main()