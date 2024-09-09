import pandas as pd
from typing import Optional,List
from gwasstudio import logger

def extract_snp(
        tiledb_data: pd.DataFrame, 
        pvalue_file_list: str,
        column_list_select: List[str] = ["sample","contig","pos_start","pos_end","id","alleles","fmt_BETA","fmt_SE","fmt_LP"],
        samples_list: List[str] = None,
        sample_partitions: Optional[int] = None,
        output_path: str = "filtered_summary_statistics.parquet",
        map_attributes: dict = None
        ) -> pd.DataFrame:
    """
    Extracting a set of SNPs from set of summary statistics in TileDBVCF dataset
    Returns a series of parquet files with the SNPs information extracted.
    :param tiledb_data: TileDBVCF data (default: None)
    :param pvalue_file_list: A txt file with a column containing the SNP ids  (default: None)
    :param column_list_select: A series of columns to extract from TileDB (default: ["sample","contig","pos_start","pos_end","id","alleles","fmt_BETA","fmt_SE","fmt_LP"])
    :param samples_list: A list of samples to extract from TileDB (default: None)
    :param sample_partitions: Number of partitions to split the samples (default: None)
    :param output_path: Path to save the filtered summary statistics (default: "filtered_summary_statistics.parquet")
    :param map_attributes: A dictionary mapping the column names in the TileDB dataset to the column names in the output (default: None)
    :return: DataFrame with the SNP information
    """
    # Read the SNP list
    snp_df = pd.read_csv(pvalue_file_list, header=None, names=['SNP'])
    snp_df['chrom'], snp_df['position'], snp_df['A1'], snp_df['A2'] = snp_df['SNP'].str.split(":",expand=True)[0], snp_df['SNP'].str.split(":",expand=True)[1], snp_df['SNP'].str.split(":",expand=True)[2], snp_df['SNP'].str.split(":",expand=True)[3]
    snp_bed_positions = snp_df.apply(lambda x: f"{x['chrom']}:{x['position']}-{x['position']}", axis=1).tolist()
    filtered_ddf = tiledb_data.read_dask(attrs=column_list_select,
                regions=snp_bed_positions,
                samples = samples_list,
                sample_partitions=sample_partitions)
    logger.info(f"Saving filtered summary statistics by SNPs in {output_path}")
    columns_attribute_mapping = {v: k for k, v in map_attributes.items() if v in filtered_ddf.columns}
    filtered_ddf.rename(columns=columns_attribute_mapping.values, inplace=True)
    filtered_ddf.to_parquet(output_path, engine='pyarrow', compression='snappy')
    return tiledb_data

