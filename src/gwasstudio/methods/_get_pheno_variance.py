import polars as pl
def compute_pheno_variance(df, trait_type):

    if trait_type == "binary":
        df = df.with_columns(
            ((4 * pl.col("N_CASES") * pl.col("N_CONTROLS")) / ((pl.col("N_CASES") + pl.col("N_CONTROLS"))
            )).alias("N"))
        
    median_pl = df.select(((pl.col("SE") ** 2) * pl.col("N") * (2 * pl.col("EAF") * (1 - pl.col("EAF")))).median())
    median_value = median_pl.item()
    median_str = str(median_value)
    return median_str