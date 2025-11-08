import numpy as np
import pandas as pd
from scipy import stats
from functools import reduce
from gwasstudio.methods.extraction_methods import tiledb_array_query

def _meta_analysis(tiledb_array, trait_list, out_prefix=None, **kwargs):
    """
    Meta-analysis for two GWAS traits using inverse variance method.
    
    Parameters:
    -----------
    trait_list: list
        A list with a series of trait ID to run the extraction on TileDB

    tiledb_array: The URI to run tiledb
    
    Returns:
    --------
    pandas DataFrame with meta-analysis results
    """
    
    # Ensure both dataframes have the required columns
    merged_list = []
    attributes = kwargs.get("attributes")
    attributes, tiledb_query = tiledb_array_query(tiledb_array, attrs = attributes)
    for trait in trait_list:
        df = tiledb_query.df[:, trait, :]
        df["SNP"] = df["CHR"].astype(str) + ":" + df["POS"].astype(str) + ":" + df["EA"] + ":" + df["NEA"]
        merged_list.append(df)
    
    merged_df = reduce(
        lambda left, right_i: pd.merge(
            left,
            right_i[1]
                .add_suffix(f"_{right_i[0]+1}")
                .rename(columns={f"SNP_{right_i[0]+1}": "SNP"}),
            on="SNP",
            how="outer"
        ),
        enumerate(merged_list[1:]),
        merged_list[0]
    )

    variant_names = merged_df['SNP'].values
    # Include first dataframe columns (no suffix) + subsequent suffixed columns
    effect_sizes = np.column_stack(
        [merged_df['BETA'].values] +
        [merged_df[f'BETA_{i+1}'].values for i in range(1,len(trait_list)-1)]
        )

    standard_error = np.column_stack(
        [merged_df['SE'].values] +
        [merged_df[f'SE_{i}'].values for i in range(1,len(trait_list)-1)]
    )

    #sample_sizes = np.column_stack(
    #    [merged_df['N'].values] +
    #    [merged_df[f'N_{i}'].values for i in range(1, len(trait_list)-1)]
    #)

    trait_names = [merged_df['TRAITID'].iloc[0]] + [
        merged_df[f'TRAITID{i}'].iloc[0] for i in range(1, len(trait_list)-1)]
    
    # Remove variants where both studies have NaN values
    not_all_nan = ~np.all(np.isnan(effect_sizes), axis=1)
    effect_sizes = effect_sizes[not_all_nan]
    standard_error = standard_error[not_all_nan]
    #sample_sizes = sample_sizes[not_all_nan]
    variant_names = variant_names[not_all_nan]
    
    # Calculate variance
    variance = standard_error ** 2
    # Weight effect sizes by inverse variance
    effect_size_divided_by_variance = effect_sizes / variance
    effect_size_divided_by_variance_total = np.nansum(effect_size_divided_by_variance, axis=1)
    
    # Calculate total weight
    one_divided_by_variance = 1 / variance
    one_divided_by_variance_total = np.nansum(one_divided_by_variance, axis=1)
    
    # Meta-analyzed effect sizes and standard errors
    meta_analysed_effect_sizes = effect_size_divided_by_variance_total / one_divided_by_variance_total
    meta_analysed_standard_error = np.sqrt(1 / one_divided_by_variance_total)
    
    # Calculate heterogeneity (I²)
    effect_size_deviations_from_mean = np.power(
        effect_sizes - meta_analysed_effect_sizes[:, np.newaxis], 2)
    
    effect_size_deviations = np.nansum(
        one_divided_by_variance * effect_size_deviations_from_mean,
        axis=1)
    
    degrees_of_freedom = (np.sum(~np.isnan(effect_size_deviations_from_mean), axis=1) - 1)
    
    with np.errstate(divide='ignore', invalid='ignore'):
        i_squared = (
            ((effect_size_deviations - degrees_of_freedom)
             / effect_size_deviations
             ) * 100)
        
        # Handle cases where I² is NaN
        i_squared[np.isnan(i_squared)] = 100
        i_squared[i_squared < 0] = 0  # I² cannot be negative
    
    # Calculate meta-analysis p-values
    z_scores = meta_analysed_effect_sizes / meta_analysed_standard_error
    meta_p_values = 2 * (1 - stats.norm.cdf(np.abs(z_scores)))
    
    # Total sample size
    #meta_analysis_sample_sizes = np.nansum(sample_sizes, axis=1)
    
    # Create results dataframe
    results_df = pd.DataFrame({
        'SNP': variant_names,
        'TRAITID': "_".join(trait_names),
        'BETA': meta_analysed_effect_sizes,
        'SE': meta_analysed_standard_error,
        'P': meta_p_values,
        #'N': meta_analysis_sample_sizes,
        'I_SQUARED': i_squared,
        'Z_SCORE': z_scores
    })
    return results_df