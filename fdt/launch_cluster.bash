#!/bin/bash
name=daskshd
walltime=2:00:0
account=ip005
qos=standard
partition=cosma7
py_script=/cosma/home/ip005/dc-mcka1/fdt/dask_cluster.py

cat <<EOF > launch_cluster.sbatch
#!/bin/bash --login
# Slurm job options (name, compute nodes, job time)
#SBATCH --job-name=$name
#SBATCH --time=$walltime
#SBATCH --ntasks=1
#SBATCH -o out.%J.out
#SBATCH -e err.%J.err
#SBATCH --exclusive
#SBATCH --mail-type=END
#SBATCH --mail-user=d.mckay@epcc.ed.ac.uk

# Replace [budget code] below with your budget code (e.g. t01)
#SBATCH --account=$account
# We use the "standard" partition as we are running on CPU nodes
#SBATCH --partition=$partition

# Set the number of threads to 1
#   This prevents any threaded system libraries from automatically
#   using threading.
export OMP_NUM_THREADS=1

module purge

source /cosma/home/ip005/dc-mcka1/lsst_stack/loadLSST.bash
setup lsst_distrib

python $py_script --account ip005 \
    --queue cosma7 \
    --num-cpus-per-node 28 \
    --threads-per-worker 1 \
    --memory 500GB \
    --walltime 02:00:00 \
    --num-nodes 5
EOF

sbatch launch_cluster.sbatch

