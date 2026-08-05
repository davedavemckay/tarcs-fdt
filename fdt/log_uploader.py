# TARCS Log Uploader
# This script uploads log files to S3.

import argparse
import s3fs
import time
import os
import sys
import tarfile
from io import BytesIO
import urllib.parse


def upload_logs(bucket=None, fs=None, workdir=None, jobid=None):
    assert bucket and fs and workdir and jobid, "Bucket, filesystem, work directory, and job ID must be provided for log upload"
    log_files = []

    for log_ext in ['.log', '.out', '.err']:
        log_files.extend([f for f in os.listdir(workdir) if f.endswith(log_ext) and jobid in f])
    tar_buffer = BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode='w:gz') as tar:
        for log_file in log_files:
            tar.add(os.path.join(workdir, log_file), arcname=log_file)
    tar_buffer.seek(0)

    s3 = fs
    key = f'jobid/{jobid}_logs.tar.gz'
    s3_uri = f"s3://{bucket}/{key}"
    
    print(f"Uploading logs to {s3_uri}")
    s3 = fs
    # write as raw YAML
    with s3.open(f"{bucket}/{key}", "wb") as f:
        f.write(tar_buffer.read())
    
    return True  

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launch a Dask SLURM Cluster")
    parser.add_argument("--s3-bucket", default="tarcs", help="S3 bucket to upload logs to")
    parser.add_argument("--jobid", required=True, help="Job ID for log file naming")
    parser.add_argument("--s3-endpoint-url", default='https://somerville.ed.ac.uk:6780', help="S3 endpoint URL for uploading scheduler address")
    parser.add_argument("--workdir", default='/work/dc164/dc164/dmlsstdev/runs', help="Working directory for the cluster")
    parser.add_argument("--creds-file", default='.aws/credentials', help="File containing AWS credentials")

    args = parser.parse_args()

    workdir = args.workdir
    if not os.path.exists(workdir):
        os.makedirs(workdir)
    elif os.path.isfile(workdir):
        raise ValueError(f"Specified workdir {workdir} is a file, please specify a directory")
    os.chdir(workdir)

    with open(args.creds_file, 'r') as credf:
        creds = {
            l.split(' = ')[0].strip():l.split(' = ')[1].strip() for l in credf.readlines() if l.startswith('aws')
        }
    creds['endpoint_url'] = args.s3_endpoint_url
    tarcs_s3 = s3fs.S3FileSystem(
        key=creds['aws_access_key_id'],
        secret=creds['aws_secret_access_key'],
        endpoint_url=creds['endpoint_url']
    )


    upload_logs(
        bucket=args.s3_bucket,
        fs=tarcs_s3,
        workdir=workdir,
        jobid=args.jobid
    )
