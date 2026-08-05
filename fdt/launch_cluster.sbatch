#!/bin/bash --login
# Slurm job options (name, compute nodes, job time)
#SBATCH --job-name=daskshd
#SBATCH --time=0:05:0
#SBATCH --ntasks=1
#SBATCH -o dask-cluster-%J.out
#SBATCH -e dask-cluster-%J.err
#SBATCH --exclusive
#SBATCH --mail-type=END
#SBATCH --mail-user=d.mckay@epcc.ed.ac.uk

#SBATCH --account=ip005
#SBATCH --partition=cosma7

# Set the number of threads to 1
#   This prevents any threaded system libraries from automatically
#   using threading.
export OMP_NUM_THREADS=1

#module purge

source /cosma/apps/ip005/dc-mcka1/loadLSST.bash
setup lsst_distrib
date
which python
python /cosma/home/ip005/dc-mcka1/tarcs-fdt/fdt/dask_cluster.py --account ip005     --queue cosma7     --workers-per-node 28     --threads-per-worker 1     --memory 500GB     --walltime 02:00:00     --num-nodes 5 --workdir /cosma/home/ip005/dc-mcka1/work --interface ibp94s0
date
