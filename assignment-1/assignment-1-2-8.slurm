#!/bin/bash
# Created by the University of Melbourne job script generator for SLURM
# Wed Mar 24 2021 23:43:44 GMT+0800 (中�^۽�^��^�^��^׶�^״)

# Multithreaded (SMP) job: must run on one node
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4

# The maximum running time of the job in days-hours:mins:sec
#SBATCH --time=0-0:5:00

# Run the job from the directory where it was launched (default)

# The modules to load:
module load foss/2019b
module load python/3.7.4

# The job command(s)
time srun -n 8 python assignment-1.py tinyTwitter.json
time srun -n 8 python assignment-1.py smallTwitter.json
time srun -n 8 python assignment-1.py bigTwitter.json