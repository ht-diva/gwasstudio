#!/bin/bash
#SBATCH --job-name=sbayes_compute    # Job name
#SBATCH --output=gwas_ingest.out    # Output file for each array task
#SBATCH --error=gwas_ingest.err     # Error file for each array task
#SBATCH --time=18:00:00
#SBATCH --cpus-per-task=10
#SBATCH --mem=100G

# Load the required environment
source /ssu/gassu/miniconda3/etc/profile.d/conda.sh
conda activate gwasstudio

# Log start time
echo "Job started at: $(date)"
start_time=$(date +%s)

gwasstudio --cores-per-worker 2 --workers 5 --memory-per-worker 20GiB ingest --file-path metadata_ingest_10.txt --uri test_ukb_ingestion_10

# Log end time
end_time=$(date +%s)
echo "Job finished at: $(date)"

# Calculate total runtime
runtime=$((end_time - start_time))
echo "Total runtime: $((runtime / 3600))h $(((runtime % 3600) / 60))m $((runtime % 60))s"

