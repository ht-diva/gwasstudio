#!/bin/bash
#SBATCH --job-name=sbayes_compute
#SBATCH --output=gwas_ingest_%A_%a.out
#SBATCH --error=gwas_ingest_%A_%a.err
#SBATCH --array=21-220:20
#SBATCH --time=18:00:00
#SBATCH --cpus-per-task=12
#SBATCH --mem=60G

i=${SLURM_ARRAY_TASK_ID}

source /ssu/gassu/miniconda3/etc/profile.d/conda.sh
conda activate gwasstudio
mkdir test_ukb_ingestion_${i}
echo "Job started at: $(date)"
start_time=$(date +%s)

gwasstudio --workers 2 --cores-per-worker 5 --memory-per-worker 20GiB ingest \
  --file-path metadata_ingest_${i}.txt \
  --uri test_ukb_ingestion_${i}

end_time=$(date +%s)
echo "Job finished at: $(date)"
runtime=$((end_time - start_time))
echo "Total runtime: $((runtime / 3600))h $(((runtime % 3600) / 60))m $((runtime % 60))s"

