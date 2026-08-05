#!/bin/bash
name=daskshd
walltime=0:20:0
account=dc164
queue=standard
qos=standard
py_script=/work/$account/$account/$USER/tarcs/somerville-integration/cirrus-side/dask_cluster.py

cat <<EOF > launch_cluster.sbatch
#!/bin/bash --login
# Slurm job options (name, compute nodes, job time)
#SBATCH --job-name=$name
#SBATCH --time=$walltime
#SBATCH --tasks=1
#SBATCH --cpus-per-task=2

# Replace [budget code] below with your budget code (e.g. t01)
#SBATCH --account=$account
# We use the "standard" partition as we are running on CPU nodes
#SBATCH --partition=$queue
# We use the "standard" QoS as our runtime is less than 4 days
#SBATCH --qos=$qos

# Set the number of threads to 1
#   This prevents any threaded system libraries from automatically
#   using threading.
export OMP_NUM_THREADS=1

export WORK=${HOME/home/work}
export HOME=$WORK

cd $WORK
source \$WORK/.bash_login

cd $WORK/runs

source /work/dc164/dc164/shared/lsst/lsst_stack/w_2026_11/loadLSST.sh
setup lsst_distrib

python $py_script --account dc164 \
    --queue standard \
    --num-cpus-per-node 280 \
    --threads-per-worker 2 \
    --memory 720GB \
    --walltime 00:10:00 \
    --num-nodes 1 \
    --s3-uri s3://tarcs/dask_scheduler_info.yaml \
    --s3-endpoint-url https://somerville.ed.ac.uk:6780
EOF

sbatch launch_cluster.sbatch