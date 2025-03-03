import polars as pl


def compute_pheno_variance(df):
    df_pl = pl.from_pandas(df)
    median_pl = df_pl.select(((pl.col("SE") ** 2) * pl.col("N") * 2 * pl.col("AF") * (1 - pl.col("AF"))).median())
    median_value = median_pl.item()
    median_str = str(median_value)
    return median_str
