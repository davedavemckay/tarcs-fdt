# TARCS Dask Cluster Launcher
# This script sets up a Dask cluster using SLURM and reports its Scheduler IP address and dashboard link.
# To be run on a single node in the cluster, which will act as the Dask scheduler.
# Worker nodes will be launched by SLURM and will connect back to this scheduler node.

from dask_jobqueue import SLURMCluster
from dask.distributed import Client
import argparse
import s3fs
import time
import os
import sys

def launch_cluster(queue='', account='', workers_per_node=0, threads_per_worker=0, memory='', walltime='', interface='', num_nodes=0, s3_uri=None, fs=None):
    # Define the SLURM cluster configuration
    cluster = SLURMCluster(
        cores=threads_per_worker*workers_per_node,
        processes=workers_per_node,
        # job_cpu=580, # set to max CPUs per node on Cirrus to ensure we get full nodes; Dask will manage how many workers/threads to run based on the cpus_per_node, workers_per_node, and threads_per_worker settings
        queue=queue,
        account=account,
        job_directives_skip=['--mem'], # memory specification required by Dask, but not allowed on Cirrus
        memory=memory,
        walltime=walltime,
        interface=interface,  # Adjust based on network interface
        job_extra_directives=['--nodes=1','--qos="standard"','--exclusive'],
        python=f'time srun {sys.executable}', # get path for current python interpreter
        shebang="#!/bin/bash --login",
    )

    # Scale the cluster to num_workers workers
    cluster.scale(jobs=num_nodes)

    # Connect a Dask client to the cluster
    client = Client(cluster)

    client.write_scheduler_file(f"{os.environ['WORK']}/dask_scheduler.json")

    print(f"Dashboard link: {client.dashboard_link}")
    print(f"Scheduler address: {cluster.scheduler_address}")

    if s3_uri and fs:
        import urllib.parse
        parsed = urllib.parse.urlparse(s3_uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        s3_uri = f"s3://{bucket}/{key}"
        print(f"Uploading Dask Scheduler address and Dashboard link to {s3_uri}")
        s3 = fs
        # write as raw YAML
        with s3.open(f"{bucket}/{key}", "w") as f:
            f.write(f'dask_scheduler_address: {cluster.scheduler_address}\n')
            f.write(f'dask_dashboard_address: {client.dashboard_link}')
            f.write("\n")

    print("Waiting for workers...")
    
    # Example computation to verify cluster is working
    
    return cluster

def walltime_to_seconds(walltime):
    h, m, s = map(int, walltime.split(':'))
    return h * 3600 + m * 60 + s - 60 # Subtract 60 seconds to ensure we stop before the walltime expires

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launch a Dask SLURM Cluster")
    parser.add_argument("--queue", default="standard", help="SLURM queue")
    parser.add_argument("--account", required=True, help="SLURM account")
    parser.add_argument("--workers-per-node", type=int, default=14, help="Number of Dask workers per node") # default to 140 workers per node; i.e., if --num-cpus-per-node is 280 and --threads-per-worker is 2, this will give 280 workers per node using 560 threads in total
    parser.add_argument("--threads-per-worker", type=int, default=20, help="Threads per worker") # default to 20 threads per worker; i.e., if --num-cpus-per-node is 280, this will give 280 workers per node using 520 threads in total
    parser.add_argument("--memory", default="720GB", help="Memory per node (cluster object)") # default Cirrus node with some headroom
    parser.add_argument("--walltime", default="01:00:00", help="Walltime")
    parser.add_argument("--interface", default="hsn0", help="Network interface, default is 'hsn0' for HPE Slingshot")
    parser.add_argument("--num-nodes", type=int, default=1, help="Number of nodes")
    parser.add_argument("--s3-uri", help="S3 URI to upload scheduler address (e.g. s3://bucket/key)")
    parser.add_argument("--s3-endpoint-url", default='https://somerville.ed.ac.uk:6780', help="S3 endpoint URL for uploading scheduler address")
    parser.add_argument("--workdir", default='/work/dc164/dc164/dmlsstdev/runs', help="Working directory for the cluster")

    args = parser.parse_args()

    workdir = args.workdir
    if not os.path.exists(workdir):
        os.makedirs(workdir)
    elif os.path.isfile(workdir):
        raise ValueError(f"Specified workdir {workdir} is a file, please specify a directory")
    os.chdir(workdir)

    if args.s3_uri:
        with open('../.aws/credentials', 'r') as credf:
            creds = {
                l.split(' = ')[0].strip():l.split(' = ')[1].strip() for l in credf.readlines() if l.startswith('aws')
            }
        creds['endpoint_url'] = args.s3_endpoint_url
        tarcs_s3 = s3fs.S3FileSystem(
            key=creds['aws_access_key_id'],
            secret=creds['aws_secret_access_key'],
            endpoint_url=creds['endpoint_url']
        )


    launch_cluster(
        queue=args.queue,
        account=args.account,
        workers_per_node=args.workers_per_node,
        threads_per_worker=args.threads_per_worker,
        memory=args.memory,
        walltime=args.walltime,
        interface=args.interface, 
        num_nodes=args.num_nodes,
        s3_uri=args.s3_uri,
        fs=tarcs_s3 if args.s3_uri else None
    )

    time.sleep(walltime_to_seconds(args.walltime))  # Keep the script running to allow workers to connect and for the dashboard to be accessible