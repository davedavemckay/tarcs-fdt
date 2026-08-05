#!/bin/bash -l

#SBATCH --ntasks 512
#SBATCH -J job_name
#SBATCH -o standard_output_file.%J.out
#SBATCH -e standard_error_file.%J.err
#SBATCH -p cosma7
#SBATCH -A project
#SBATCH --exclusive
#SBATCH -t 72:00:00
#SBATCH --mail-type=END                          # notifications for job done & fail
#SBATCH --mail-user=<email address>

module purge
#load the modules used to build your program.
module load intel_comp
module load intel_mpi
module load hdf5


# Run the program

mpirun -np $SLURM_NTASKS your_program your_inputs

