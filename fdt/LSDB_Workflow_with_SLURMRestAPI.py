#!/usr/bin/env python
# coding: utf-8

# Converted from TARCs LSDB_Workflow_with_SLURMRestAPI.ipynb to .py on 2026-08-06

# ## SLURM API / Rubin DP1 Workflow Example

# ### Table of Contents
# 
# 
# | Section | Title |
# |---------|-------|
# | 1   | Introduction |
# | 2.1 | IPython Notebook setup | 
# | 2.2 | SLURMRestAPI Basics |
# | 2.3 | Data transfer - S3 setup |
# | 2.4 | Job 1 - Dask SLURMCluster |
# | 2.5 | More SLURMRestAPI tips |
# | 3.1 | Prepare and stage Rubin DP1 LSDB data to S3 |
# | 3.2 | Job 2 - Rubin DP1 LSDB Cone Search |
# | 3.3 | Retrieving outputs and logs via S3 |
# | 3.4 | Job 3 - Submit multiple simultaneous execution cone search jobs |
# | 4.1 | DIA Objects and light curve (LC) plots |
# | 4.2 | Staging a larger dataset |
# | 4.3 | Notebook (cone search) and Job 4 Cirrus (6 light curves) |
# | 5   | Clean-up |
# | 6   | Conclusions |
# 

# ### 1. Introduction
# 
# The purpose of this tutorial is to provide an example of an astronomical data analysis workflow using the SLURMRestAPI available for job submission to Cirrus, from the Notebooks Service provided by the Rubin Science Platform (RSP) on Somerville.
# 
# This will demonstrate:
# 
# - credentials setup for SLURMRestAPI and an example S3 object storage for data transfer;
# - launching a Dask SLURMCluster on Cirrus for execution of data processing payloads;
# - examples of data processing payloads;
# - how to clean up Cirrus jobs when finished. 
# 
# The LSDB form of the Vera Rubin Observatory Data Preview 1 (Rubin DP1) dataset will be used. Use of this data is subject to [Rubin Data Rights](https://www.lsst.org/content/data-rights).
# 
# A copy of the LSST Science Pipelines code and a clone of this repo/branch has been placed on Cirrus separately (jobs will be based on the scripts contained in the `somerville-integration/cirrus-side` folder). Therefore, while job scheduling is performed remotely through the SLURMRestAPI, it is expected that code to be executed is already in place on Cirrus. Data will be transferred via S3 (push from Somerville, pull from Cirrus, and vice versa) where it is assumed the user of a research cloud-based will be able to use S3-compatible object storage either on the same service or at a third site.

# ### 2.1. IPython Notebook setup

# LSST/LSDB imports
# 
# For reference, see https://lsdb.io/



import lsdb
import astropy.units as u
from astropy.coordinates import SkyCoord
import pandas as pd
# get_ipython().run_line_magic('matplotlib', 'widget')
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from mpl_toolkits.axes_grid1 import ImageGrid
from matplotlib.animation import FuncAnimation
# from IPython.display import HTML, Image, display
import numpy as np


# System/S3/SLURM imports

# For reference, see https://slurm.schedmd.com/rest_api.html



# system
import os
import time
from pathlib import Path
from upath import UPath
import getpass
# s3
# import s3fs # this example will not use s3
# slurm
import requests
import json
import jmespath
import yaml
# local Dask
import dask
from dask.distributed import Client, as_completed, wait, get_client


# A few settings to keep the cell outputs clean
dask.config.set({"logging.distributed": "critical"})

import logging

# This also has to be done, for the above to be effective
logger = logging.getLogger("distributed")
logger.setLevel(logging.CRITICAL)

import warnings

# Finally, suppress the specific warning about Dask dashboard port usage
warnings.filterwarnings("ignore", message="Port 8787 is already in use.")
warnings.filterwarnings("ignore", message="UserWarning: Dask currently has limited support for converting pandas extension dtypes to arrays. Converting int64[pyarrow] to object dtype.")


# The "local" Dask Cluster is accessed via the Client object, below. This is a single worker with 8 threads that will run in the background of this Notebook.



client = Client(n_workers=1, threads_per_worker=8, memory_limit='14GiB')
print(client)


# This Cluster can be monitored by clicking the link below.



url = f'https://rsp.lsst.ac.uk/nb/user/{getpass.getuser()}/{client.dashboard_link}'
print(url)


# ### 2.2. SLURMRestAPI Basics

# To access the SLURMRestAPI, we use the Python `requests` and `json` packages, plus the `os` package to access the credentials file and work with environment variables.



with open(os.path.expanduser("~/.tokens/slurm"), "r") as f:
    SLURM_JWT = f.read().strip().split("=")[1]
os.environ["SLURM_JWT"] = SLURM_JWT
os.environ["FI_CXI_RX_MATCH_MODE"] = "hybrid"
os.environ["SBATCH_EXPORT"] = "FI_CXI_RX_MATCH_MODE,SBATCH_EXPORT"
SLURM_addr = "http://172.24.38.181/slurm/v0.0.40"
SLURMdb_addr = "http://172.24.38.181/slurmdb/v0.0.40"
cirrus_username = "dmlsstdev"
headers = {
    "X-SLURM-USER-TOKEN": SLURM_JWT,
    "X-SLURM-USER-NAME": cirrus_username
}


# To confirm the setup, we'll get some basic user stats



response = requests.get(f"{SLURM_addr}/diag", headers=headers)
diag_info = response.json()
my_stats = jmespath.search(f"statistics.rpcs_by_user[?user=='{cirrus_username}']", diag_info)
print(json.dumps(my_stats[0], indent=2))







# For most interactions with the SLURM API we will just use `requests` directly, but for submitting jobs we'll define a function, `submit_job()` for ease of use.
# 
# To mimic the `sbatch` command-line utility, used on the Cirrus login nodes, `submit_job()` returns the `job_id` of the submitted job.
# 
# It is worth noting that the SLURM API documentation (https://slurm.schedmd.com/rest_api.html) labels some fields as "optional" - this means it is optional to the API call type, but not necessarily optional to the target compute cluster. Comments in the cell below mark required fields for Cirrus.



def submit_job(
    script=None,
    workdir="/work/dc164/dc164/dmlsstdev/runs",
    name='',
    account=None,
    partition='standard',
    qos='standard',
    tasks=1,
    cpus_per_task=1,
    time_limit_number=10,
    exclusive=True,
    comment="Testing SLURM API",
    dependency=''
):
    """
    Submit a batch job to the SLURM REST API and return the submitted job ID.

    Parameters
    ----------
    script : str
        Shell script content to execute on the cluster.
    workdir : str, default "/work/dc164/dc164/dmlsstdev/runs"
        Working directory for the submitted job.
    name : str, default ''
        Job name shown in SLURM.
    account : str
        SLURM account to charge the job to.
    partition : str, default 'standard'
        SLURM partition/queue name.
    qos : str, default 'standard'
        SLURM quality-of-service setting.
    tasks : int, default 1
        Number of tasks for the job.
    cpus_per_task : int, default 1
        Number of CPUs allocated per task.
    time_limit_number : int, default 10
        Time limit in minutes.
    exclusive : bool, default True
        Whether to request exclusive node allocation.
    comment : str, default "Testing SLURM API"
        Optional SLURM job comment.
    dependency : str, default ''
        SLURM dependency expression (e.g. "afterok:12345").

    Returns
    -------
    int | None
        Submitted SLURM job ID if successful, otherwise None.

    Notes
    -----
    Uses notebook-scoped `SLURM_addr` and `headers` values for authentication
    and endpoint routing.
    """
    assert script is not None and account is not None, "Script and account must be provided to submit a job"

    job_desc = {
        "name": name,
        "account": account,  # required
        "comment": comment,
        "partition": partition,  # required
        "qos": qos,  # required
        "tasks": tasks,
        "cpus_per_task": cpus_per_task,
        "time_limit": {  # required
            "set": True,
            "infinite": False,
            "number": time_limit_number,
        },
        "contiguous": False,  # required (despite being default)
        "exclusive": exclusive,
        "current_working_directory": workdir,  # required
        "environment": [  # required
            "FI_CXI_RX_MATCH_MODE=hybrid",
            "SBATCH_EXPORT=FI_CXI_RX_MATCH_MODE,SBATCH_EXPORT",
        ],
        "dependency": dependency,
    }

    job = {
        "script": script,
        "job": job_desc,
    }

    response = requests.post(f"{SLURM_addr}/job/submit", headers=headers, json=job)
    return response.json().get("job_id", None)


# ### 2.3. Data transfer - S3 setup

# While the SLURMRestAPI gives access to the SLURM scheduler on Cirrus, we do not have direct access to the Cirrus filesystem.
# 
# One way to get around this is by using the S3-compatible Object Store on Somerville, whereby this Notebook and Cirrus jobs can push and pull data using S3 credentials.

# The below reads EC2 credentials produced through the Openstack WebUI. We will access an Object Store Container, `tarcs`.



with open(os.path.expanduser('~/.aws/credentials'), 'r') as credf:
    creds = {
        l.split(' = ')[0].strip():l.split(' = ')[1].strip() for l in credf.readlines() if l.startswith('aws')
    }
tarcs_s3 = s3fs.S3FileSystem(
      key=creds['aws_access_key_id'],
      secret=creds['aws_secret_access_key'],
      endpoint_url='https://somerville.ed.ac.uk:6780'
   )
bucket_name = 'tarcs'




print(tarcs_s3.ls(bucket_name))


# Now that our data is uploaded, we will launch a Dask Cluster on Cirrus.

# ### 2.4. Job 1 - Dask SLURMCluster - and more SLURMRestAPI tips
# 
# We can now define and submit a Dask Cluster job, which runs `tarcs/somerville-integration/cirrus-side/dask_cluster.py` with parameters for 280 2-thread workers running on a single node. Note the scheduler runs on its own node and itself submits workers via local SLURM calls.
# 
# Note: local use of LSDB (i.e., in this Notebook) uses a LocalCluster for Dask workloads internally, whereas LSDB processes we will run on Cirrus later will use the dask_jobqueue.SLURMCluster we are launching now. See [dask_cluster.py](../cirrus-side/dask_cluster.py).

# As we will reuse variable names, such as `script`, it's best to change values and submit in the same cell.



walltime_hours = 2
walltime_minutes = 30


script = f"""#!/bin/bash --login
date
source /etc/bashrc
export WORK=/work/dc164/dc164/{cirrus_username}
export HOME=${{WORK}}
source $WORK/../shared/lsst/lsst_stack/w_latest/loadLSST.bash
setup lsst_distrib
py_script=$WORK/tarcs/somerville-integration/cirrus-side/dask_cluster.py
python $py_script --account dc164 \
    --queue standard \
    --workers-per-node 14 \
    --threads-per-worker 20 \
    --memory 720GB \
    --walltime {str(walltime_hours).zfill(2)}:{str(walltime_minutes).zfill(2)}:00 \
    --num-nodes 1 \
    --s3-uri s3://tarcs/dask_scheduler_info.yaml \
    --s3-endpoint-url https://somerville.ed.ac.uk:6780
date
"""
name = "DaskShd"
account = "dc164"
dask_clust_jobid = submit_job(script=script, name=name, account=account, time_limit_number=walltime_hours*60+walltime_minutes)
print(dask_clust_jobid)


# ### 2.5. More SLURMRestAPI tips

# We can monitor the Dask Scheduler job by querying SLURMdb with `dask_clust_jobid`



dask_clust_job_info = requests.get(f"{SLURMdb_addr}/job/{dask_clust_jobid}", headers=headers)
print(json.dumps(dask_clust_job_info.json(), indent=2))




# Or, for a more readable output:
print(f"Dask Scheduler, job {dask_clust_jobid}, is {dask_clust_job_info.json()['jobs'][0].get('state').get('current')[0]}.")


# Alternatively, the `/job/state` query gives a list of all jobs, but only includes their ID and current state, so is a little less information-dense.
# 
# This is particularly useful when submitting multiple jobs by looping over a local `List`, which we'll see later.



all_slurm_job_states = requests.get(f"{SLURM_addr}/jobs/state", headers=headers)
print(json.dumps(all_slurm_job_states.json(), indent=2))




my_job = [ j for j in all_slurm_job_states.json()['jobs'] if j['job_id'] == str(dask_clust_jobid) ][0]
print(f"Dask Scheduler, job {dask_clust_jobid}, is {my_job['state'][0]}.")


# _Before continuing_
# 
# If the job "DaskShd" is "PENDING" (or running cells causes any errors), re-run the cells above until it is "RUNNING".

# The above works well for the Dask Scheduler job, but for the Dask Worker(s), we don't know the job ID(s) yet.
# 
# To see our Dask workers, we now need to search through jobs for those owned by `cirrus_username`.
# 
# __Warning:__ Depending on the size of the `slurmrestd` database, this can take several seconds, so it's best not to run this cell too many times.
# 
# This is equivalent to running `squeue` (or `scontrol` on all jobs!) as it gives all information on every PENDING or RUNNING, job.



all_jobs = requests.get(f"{SLURM_addr}/jobs", headers=headers)


# The response to this query is also large, and utilising the JSON output, a large Python `List` of `Dict` objects representing each job, will be slow, so here we use Pandas to take advantage of its pre-compiled, vectorised operations.



jobs = pd.DataFrame.from_dict(all_jobs.json().get("jobs", []))
# Now that we have a Pandas DataFrame object, we do not need to hold the full response in memory
del all_jobs




# We will further reduce the load on memory by reducing the size of the DataFrame to just the information we want
jobs = jobs[['account','job_id','name','job_state','cpus','node_count','exclusive','user_name']] # limit to the columns we need
jobs = jobs[jobs['user_name'] == cirrus_username] # filter by user name
jobs['job_state'] = [ j[0] for j in jobs['job_state'].values ] # unpack job_state values, which are returns as lists of single strings, for easier use downstream
print(jobs)




print(jobs[jobs['job_state'].isin(['RUNNING','PENDING'])])


# Above, jobs with a "`[RUNNING]`" job state and name "`dask-worker`" represent our Dask Cluster worker nodes and "`DaskShd`" is the Dask Scheduler.

# As the "DaskShd" job is running, its scheduler info will have been publised to S3.
# Note: this is also written to a file on Cirrus where other jobs can pick it up.



print(tarcs_s3.glob('tarcs/*'))




dl_response = tarcs_s3.get('tarcs/dask_scheduler_info.yaml', 'dask_scheduler_info.yaml')
if dl_response == [None]:
    print('Download successful.')


# We can use the Python `yaml` package to read this information into a `dict`



dask_scheduler_info = yaml.safe_load(open('dask_scheduler_info.yaml'))
print(dask_scheduler_info)




dask_cluster_ip = dask_scheduler_info['dask_scheduler_address'].split('//')[-1]
print(dask_cluster_ip)


# _Note: Monitoring the Dask Dashboard on Cirrus requires port forwarding through SSH, and so is beyond the scope of this tutorial._
# 
# However, if you do have SSH access, follow the below:



dashboard_addr = dask_scheduler_info['dask_dashboard_address'].split('//')[-1].split('/')[0]
dashboard_ip, dashboard_port = dashboard_addr.split(':')
print('1. Open a terminal and enter the following SSH port forward for Dask Dashboard.')
print(f'ssh {cirrus_username}@login.cirrus.ac.uk -L 8787:{dashboard_ip}:{dashboard_port}\n')
print('2. Click the link below to open the Dashboard forwarded to your local machine.')
print('Cirrus Dask Dashboard link: http://127.0.0.1:8787/status')


# You will now have two browser windows showing Dask Dashboards:



print(f'https://rsp.lsst.ac.uk/nb/user/{getpass.getuser()}/{client.dashboard_link} - the Dask Cluster running in this RSP Notebooks session')
print('http://127.0.0.1:8787/status - The Dask Cluster running on Cirrus')


# ### 3.1. Prepare and stage Rubin DP1 LSDB data to S3

# #### Access DP1 Object Catalog - the below is based on the Rubin LSDB DP1 Catalog Access tutorial, [102_5_LSDB_data_access.ipynb](http://github.com/lsst/tutorial-notebooks/blob/main/DP1/100_How_to_Use_RSP_Tools/102_Catalog_access/102_5_LSDB_data_access.ipynb) from (https://github.com/lsst/tutorial-notebooks)
# 
# LSDB uses the Dask lazy-loading backend (local Dask Cluster) by automatically finding the Dask Client object in the Notebook's namespace

# Set the base path to the LSDB-formatted DP1 data in the RSP.



base_path = UPath("/rubin/lsdb_data")




object_cat = lsdb.open_catalog(base_path / "object_collection")
print(object_cat)


# Note: only 42 of 1304 columns have been loaded:



print(object_cat.columns)


# `object_cat.all_columns` shows the full list:



print(object_cat.all_columns)


# We will search for and select columns that contain PSF fluxes converted to magnitudes.

# The above takes around 5 seconds, with the longest cell run being the `lsdb.open_catalog` cell.
# We will now run an `lsdb.open_catalog` step with our `psfMag_columns` to give `object_cat_selected_columns`, which is slightly faster.



psfMag_columns = ['coord_dec', 'coord_decErr', 'coord_ra', 'coord_raErr', 'g_psfFlux', 'g_psfFluxErr', 'g_psfMag', 'g_psfMagErr']




object_cat_selected_columns = lsdb.open_catalog(base_path / "object_collection", columns=psfMag_columns)




print(object_cat_selected_columns.columns)




assert all(object_cat_selected_columns.columns) == all(object_cat_selected_columns.all_columns)


# We will perform search on an area of the sky through a typical astronomical "cone search".
# 
# The original Rubin LSDB DP1 tutorial used a cone search of 0.1 arc hours and takes around 3 seconds in the Notebook (note if it is run a second time it takes around 1 second due to Python interpreter caching).
# 
# - we will instead stage the data to S3 and run the cone search on Cirrus, with an increased search radius of 1 arc hour.

# #### Stage data to S3

# - We will write psfMag-related data locally first - Dask can write to S3, but LSDB doesn't seem to expose this functionality for catalogs.
# - This will take around 30-40 seconds. This is writing data from across the whole DP1 dataset.
# - Alternatively, we could just upload the whole catalog to S3 - we will see an example of this later
# - Note: LSDB writes in heirarchical parquet format, so the output will be a folder with many subfolders



# Only run if local copy doesn't exist
if not os.path.exists('psfMag'):
    object_cat_selected_columns.write_catalog('psfMag')
else:
    print('object_cat_selected_columns already saved as psfMag')


# Upload data if not already uploaded.
# 
# Note: this is not a thorough test of whether the data already exists on S3, just a quick check for the top-level folder name, in reality, one should ensure all expected files are present.



if 'tarcs/psfMag' not in tarcs_s3.ls(bucket_name):
    response = tarcs_s3.upload('psfMag', f'{bucket_name}/psfMag', recursive=True)
    if all(r is None for r in response):
        print('Success')
else:
    print('Already present')


# ### 3.2. Job 2 - Rubin DP1 LSDB Cone Search

# #### Prepare SLURM script and submit via the SLURMRestAPI



## Cone search parameters
# Coordinates of the Extended Chandra Deep Field South
ra = 53.16
dec = -28.10
# Radius of search
r_arcsec = 1 * 3600
download_prefix = 'psfMag'
upload_prefix = 'psfMag_cone_search'
cone_search_columns = list(object_cat_selected_columns.columns)




script = f"""#!/bin/bash --login
date
source /etc/bashrc
export WORK=/work/dc164/dc164/dmlsstdev
export HOME=${{WORK}}
echo "WORK: ${{WORK}}"
echo "HOME: ${{HOME}} (should be identical to WORK)"
source ${{WORK}}/../shared/lsst/lsst_stack/w_latest/loadLSST.bash
setup lsst_distrib
echo "Python executable: `which python`"
py_script=${{WORK}}/tarcs/somerville-integration/cirrus-side/lsdb_cone_search.py
python $py_script \
    --ra {ra} \
    --dec {dec} \
    --radius {r_arcsec} \
    --s3-endpoint-url https://somerville.ed.ac.uk:6780 \
    --s3-bucket 'tarcs' \
    --download-prefix {download_prefix} \
    --upload-prefix {upload_prefix} \
    --columns {cone_search_columns} \
    --dask-cluster-ip {dask_cluster_ip}    
date
"""
name = "ConeSearch"
account = "dc164"
cone_search_jobid = submit_job(
    script=script,
    name=name,
    account=account,
)




print(cone_search_jobid)




cone_search_job_info = requests.get(f"{SLURMdb_addr}/job/{cone_search_jobid}", headers=headers)
print(f"Cone Search, job {cone_search_jobid}, is {cone_search_job_info.json()['jobs'][0].get('state').get('current')[0]}.")


# Once the above job reaches a `COMPLETED` state, run the cells below to download the plot. You will get a `[None]` response.

# ### 3.3. Retrieving outputs and logs via S3



plot_path = f'{upload_prefix}/cone_search_results-{ra}_{dec}_{r_arcsec:.1f}.png'
tarcs_s3.get(f'tarcs/{plot_path}',plot_path)




im = plt.imread(plot_path)
plt.figure()
plt.imshow(im)
plt.axis('off')
plt.savefig(plot_path)
plt.close()


# For interest: the Rubin Observatory Commissioning Camera (ComCam), on which DP1 was based, has a field-of-view of around 40 x 40 arc minutes, producing 144 megapixel images (~ 1/21 the size of an LSSTCam image at 3.2 gigapixels).
# 
# Our cone search of 1 arc hour is centered on a region ComCam was pointed at - so we are seeing an overlay (co-addition) of multiple images of this region.

# We may now want to look at the output from the job. To do that, we can run another job to upload the logs to S3.
# 
# - Note: this is submitted with a dependency on the cone search job (`afterany:<jobid>`) to ensure it runs after the output is complete.



prev_jobid = cone_search_jobid
response = requests.get(f"{SLURMdb_addr}/job/{prev_jobid}", headers=headers)
prev_job_stat = response.json()
prev_workdir = prev_job_stat.get("jobs", {})[0].get("working_directory", ["N/A"])
print(f"Previous job working directory: {prev_workdir}")
script = f"""#!/bin/bash --login
date
source /etc/bashrc
export WORK=/work/dc164/dc164/dmlsstdev
export HOME=${{WORK}}
echo "WORK: ${{WORK}}"
echo "HOME: ${{HOME}} (should be identical to WORK)"
source ${{WORK}}/../shared/lsst/lsst_stack/w_latest/loadLSST.bash
setup lsst_distrib
echo "Python executable: `which python`"
py_script=$WORK/tarcs/somerville-integration/cirrus-side/log_uploader.py
python $py_script --s3-bucket tarcs \
    --jobid {prev_jobid} \
    --workdir {prev_workdir} \
    --creds-file /work/dc164/dc164/dmlsstdev/.aws/credentials \
"""

upload_jobid = submit_job(
    script=script,
    name="LogUploader",
    account=account,
    comment="Uploading logs from previous job",
    dependency=f"afterany:{prev_jobid}",
)
print(upload_jobid)




upload_job_info = requests.get(f"{SLURMdb_addr}/job/{upload_jobid}", headers=headers)
print(f"LogUploader, job {upload_jobid}, is {upload_job_info.json()['jobs'][0].get('state').get('current')[0]}.")


# Continue once `COMPLETED`



tarcs_s3.get(f'tarcs/jobid/{prev_jobid}_logs.tar.gz', f'{prev_jobid}_logs.tar.gz')




# ls {prev_jobid}_logs.tar.gz


# We can get a quick view of this log by running a shell command:



get_ipython().system('tar xvfz {prev_jobid}_logs.tar.gz && cat slurm-{prev_jobid}.out')


# - The above should correspond to the contents of the slurm-<jobid>.out file in your working directory on Cirrus.
# - The first 100 objectIds (on which the search results catalog is indexed) are printed at the end of the cone search along with the HEALPix number as the name of the series, which determines which set of Parquet files were accessed.

# ### 3.4. Job 3 - Submit multiple simultaneous execution cone search jobs

# Just for fun, let's loop over radii and run several cone searches in parallel (using the Dask Cluster as a task farm):
# 
# - the first cone search used a radius of 1 arc hours - we will gradually narrow to 0.01 arc hours (36 arc seconds, ~180 ComCam pixels)



radii = [ r * 3600 for r in [0.75, 0.5, 0.25, 0.1, 0.075, 0.05, 0.025, 0.01] ]
print(radii)



# This will run simultaneously as separate jobs, and their compute tasks will be executed by the Dask cluster
cone_search_jobids = []
for r_arcsec in radii:
    script = f"""#!/bin/bash --login
    date
    source /etc/bashrc
    export WORK=/work/dc164/dc164/dmlsstdev
    export HOME=${{WORK}}
    echo "WORK: ${{WORK}}"
    echo "HOME: ${{HOME}} (should be identical to WORK)"
    source ${{WORK}}/../shared/lsst/lsst_stack/w_latest/loadLSST.bash
    setup lsst_distrib
    echo "Python executable: `which python`"
    py_script=${{WORK}}/tarcs/somerville-integration/cirrus-side/lsdb_cone_search.py
    python $py_script \
        --ra {ra} \
        --dec {dec} \
        --radius {r_arcsec} \
        --s3-endpoint-url https://somerville.ed.ac.uk:6780 \
        --s3-bucket 'tarcs' \
        --download-prefix {download_prefix} \
        --upload-prefix {upload_prefix} \
        --columns {cone_search_columns} \
        --dask-cluster-ip {dask_cluster_ip}    
    date
    """
    name = "ConeSearch"
    account = "dc164"
    cone_search_jobids.append(
        submit_job(
            script=script,
            name=name,
            account=account,
        )
    )




all_slurm_job_states = requests.get(f"{SLURM_addr}/jobs/state", headers=headers)
cone_search_states = []
for job_id in cone_search_jobids:
    cone_search_states.append([ j for j in all_slurm_job_states.json()['jobs'] if j['job_id'] == str(job_id) ][0]['state'][0])
    print(f"Cone Search, job {job_id}, is {cone_search_states[-1]}.")


# - again, run the above a few times, until all jobs are 'COMPLETED'
# - we won't look at the slurm-<jobid>.out files this time, but jump straight to the PNG downloads
# - the results count ranges from 494851 objects in the widest cone search, down to 133 objects in the narrowest

# Here, for checking the plots have been uploaded, we use one S3 enpoint query (`tarcs_s3.glob`) and loop over it to find PNG files, as opposed to looping over the PNG file names and using multiple S3 endpoint queries with `tarcs_s3.ls`.



print([ o for o in tarcs_s3.glob(f'tarcs/{upload_prefix}/**') if '.png' in o ])


# For download, it is necessary to use multiple S3 endpoint queries.



for r in radii:
    tarcs_s3.get(f'tarcs/{upload_prefix}/cone_search_results-{ra}_{dec}_{r:.1f}.png', f'{upload_prefix}/cone_search_results-{ra}_{dec}_{r:.1f}.png')


# - Since we've now run a set of cone searches with a range of radii, we can make a pseudo "zoom" animation!
# - First, we will add the 1 arc hour radius into the `radii` list, then we will produce animation frames using the list.



if 1.0*3600 not in radii:
    radii.insert(0, 1.0*3600)




print(radii)




fig, ax = plt.subplots()
plt.subplots_adjust(top=1, bottom=0, left=0, right=1)
ax.axis('off')

initial_im = plt.imread(f'{upload_prefix}/cone_search_results-{ra}_{dec}_{radii[0]:.1f}.png')
im_display = ax.imshow(initial_im)

def animate(r):
    new_im = plt.imread(f'{upload_prefix}/cone_search_results-{ra}_{dec}_{r:.1f}.png')
    im_display.set_data(new_im)
    return [im_display]

anim = FuncAnimation(fig, animate, frames=radii, interval=300)
plt.savefig('zoom.png')
# anim.save('zoom.gif')


# - with "Reflect" selected above, hit the play button (&#x25B6;)
# - this zooms in from a radius of ComCam image to a radius of 180 pixels showing individual galaxies

# ### 4.1. DIA Objects and light curve (LC) plots
# 
# We will now plot light curves of a few of the objects found through the cone search in the ECDFS region of the sky. We will also plot the light curve of a known supernova (SN), objectId 611255759837069401.
# 
# For this we will look at the `dia_object_collection`, where a DIA Object is defined as: an astrophysical transient or variable object at a static sky coordinate; these are objects at DIA Sources, which are sources that appear in difference images, through Difference Image Analysis (DIA).



known_sn = 611_255_759_837_069_401


# ### 4.2. Staging a larger dataset

# Preparation of data for light curve calcultion can be slow, so we will stage the whole collection to S3 to have is next to our compute.



prefix = 'dia_object_collection'
if f'{bucket_name}/{prefix}' not in tarcs_s3.ls('tarcs'):
    tarcs_s3.put(f'{base_path}/{prefix}/', f'{bucket_name}/{prefix}', recursive=True)
    print('dia_object_collection uploaded')
else:
    print('Already present')


# We need to redo the cone search to find DIA Objects in this region.

# A cone search for `diaObjectId` is fast if the radius is kept relatively small.



dia_object_cat = lsdb.open_catalog(base_path / "dia_object_collection")


# ### 4.3. Notebook (cone search) and Job 4 Cirrus (6 light curves)



cs_results = dia_object_cat.cone_search(ra, dec, 36.)['diaObjectId'].compute()


# At this radius, 92 objects are found - meaning 92 objects have been identified as having significant difference images



print(cs_results.values)


# We will compute the light curves our known SN plus for the first 5 DIA objects found in our cone search.



sn_search_ids = list(cs_results.values[:5])




sn_search_ids.insert(0, known_sn)
print(sn_search_ids)


# Submit light curve calculation jobs on Cirrus



download_prefix = 'dia_object_collection' # Note: only data missing on Cirrus will be downloaded
upload_prefix = 'DIA_LightCurves'
dia_search_jobids = []
for _id in sn_search_ids:
    script = f"""#!/bin/bash --login
    date
    source /etc/bashrc
    export WORK=/work/dc164/dc164/dmlsstdev
    export HOME=${{WORK}}
    echo "WORK: ${{WORK}}"
    echo "HOME: ${{HOME}} (should be identical to WORK)"
    source ${{WORK}}/../shared/lsst/lsst_stack/w_latest/loadLSST.bash
    setup lsst_distrib
    echo "Python executable: `which python`"
    py_script=${{WORK}}/tarcs/somerville-integration/cirrus-side/lsdb_dia_collection_lc_plot.py
    python $py_script \
        --s3-endpoint-url https://somerville.ed.ac.uk:6780 \
        --s3-bucket 'tarcs' \
        --download-prefix {download_prefix} \
        --upload-prefix {upload_prefix} \
        --dask-cluster-ip {dask_cluster_ip} \
        --dia-objectid {_id}
    date
    """
    name = "SN_Search"
    account = "dc164"
    dia_search_jobids.append(submit_job(
        script=script,
        name=name,
        account=account,
        time_limit_number=60
    ))
print(dia_search_jobids)




all_slurm_job_states = requests.get(f"{SLURM_addr}/jobs/state", headers=headers)
dia_lc_states = []
for job_id in dia_search_jobids:
    dia_lc_states.append([ j for j in all_slurm_job_states.json()['jobs'] if j['job_id'] == str(job_id) ][0]['state'][0])
    print(f"DIA Light Curve Plot, job {job_id}, is {dia_lc_states[-1]}.")


# Once the above is `COMPLETED`, the plot can be downloaded from S3 storage as below.



print(sn_search_ids)




print(tarcs_s3.glob(f'{bucket_name}/{upload_prefix}/*'))




dl = [tarcs_s3.get(f'{bucket_name}/{upload_prefix}/light_curve_{_id}.png', f'{upload_prefix}/light_curve_{_id}.png') for _id in sn_search_ids]
print(dl)




    # images.append()
get_ipython().run_line_magic('matplotlib', 'inline')
from mpl_toolkits.axes_grid1 import ImageGrid

ims = [ plt.imread(f'{upload_prefix}/light_curve_{_id}.png') for _id in sn_search_ids ]

fig = plt.figure(figsize=(4., 4.))
grid = ImageGrid(fig, (1., 1., 8., 8.),  # similar to subplot(111)
                 nrows_ncols=(2, 3),  # creates 2x2 grid of Axes
                 axes_pad=0.1,  # pad between Axes in inch.
                 )

for i, ax, im in zip([x for x in range(6)], grid, ims):
    # Iterating over the grid returns the Axes.
    ax.axis('off')
    ax.imshow(im)
    if i == 0:
        ax.set_title('Known Supernova')
    else:
        ax.set_title('Transient Object')

plt.savefig(f'{upload_prefix}/light_curves_grid.png')
plt.close()


# ### 5. Clean-up

# While each of the data analysis jobs *should* have completed, our Dask SLURMCluster will still be running on Cirrus.
# 
# - The Dask Scheduler (our first submitted job), and its Dask Worker jobs, must now be canceled (not just the scheduler job).
#   - This is done using an HTTP DELETE request
# - It is also good practice to check for any jobs that need to be cleaned up.



dask_clust_job_info = requests.get(f"{SLURMdb_addr}/job/{dask_clust_jobid}", headers=headers)
print(f"Dask Scheduler, job {dask_clust_jobid}, is {dask_clust_job_info.json()['jobs'][0].get('state').get('current')[0]}.")




all_jobs = requests.get(f"{SLURM_addr}/jobs", headers=headers)
jobs = pd.DataFrame.from_dict(all_jobs.json().get("jobs", []))
del all_jobs




jobs = jobs[['account','job_id','name','job_state','user_name']] # limit to the columns we need
jobs['job_state'] = [ j[0] for j in jobs['job_state'].values ] # unpack job_state values, which are returns as lists of single strings, for easier use downstream
jobs = jobs[jobs['user_name'] == cirrus_username] # filter by username and job_state
jobs = jobs[jobs['job_state'].isin(['RUNNING','PENDING'])]
print(jobs)


# **WARNING:** the below will kill _all_ of your jobs - ensure this is what you want!



for jobid in jobs['job_id']:
    response_json = requests.delete(f"{SLURM_addr}/job/{jobid}", headers=headers).json()
    print(json.dumps(response_json, indent=2))


# _If there are no errors or warnings, the delete command has worked. We can verify this by checking the current job state, which will be "CANCELLED" once the `slurmrestd` has cancelled the job._



for jobid in jobs['job_id']:
    job_state = requests.get(f"{SLURMdb_addr}/job/{dask_clust_jobid}", headers=headers).json()['jobs'][0].get('state').get('current')[0]
    print(job_state)


# ### 6. Conclusions

# This tutorial demonstrated the use of the SLURMRestAPI available on Cirrus, running from the Notebooks Service provided by the Rubin Science Platform (RSP) on Somerville, and using an example of an astronomical data analysis workflow.
# 
# The workflow used the LSDB form of the Vera Rubin Observatory Data Preview 1 (Rubin DP1). Use of this data is subject to [Rubin Data Rights](https://www.lsst.org/content/data-rights).
# 
# The tutorial covered:
# 
# - credentials setup for SLURMRestAPI and an example S3 object storage for data transfer;
# - launching a Dask SLURMCluster on Cirrus for execution of data processing payloads;
# - execution of code already available on Cirrus to process and analyse data provided via S3;
# - examples of data analysis payloads, including a cone search and plotting DIA object light curves;
# - how to clean up Cirrus jobs when finished.
# 
# The use of the SLURMRestAPI opens up the possibility of utilising HPC-scale compute resources from an CLoud-based, Notebook-scale, data access platform.
