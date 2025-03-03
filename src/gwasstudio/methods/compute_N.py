def compute_pheno_variance(df, phenovar):
    neff = pheno_var/((2*df["EAF"]*(1-df["EAF"])) * (df["SE"]^2))
    median_pl = df_pl.select(((pl.col("SE") ** 2) * pl.col("N") * 2 * pl.col("AF") * (1 - pl.col("AF"))).median())
    median_value = median_pl.item()
    median_str = str(median_value)
    return median_str