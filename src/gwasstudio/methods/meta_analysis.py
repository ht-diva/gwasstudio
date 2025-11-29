from functools import reduce
import numpy as np
import pandas as pd
from scipy import stats
from gwasstudio.methods.extraction_methods import tiledb_array_query


def _meta_analysis(tiledb_array, trait_list, out_prefix=None, **kwargs):
    """
    Meta-analysis for two or more GWAS traits using inverse variance method.
    """
    # Ensure both dataframes have the required columns
    merged_list = []
    collected_trait_names = []  # Collect names here to avoid NaN issues later

    attributes = kwargs.get("attributes")
    attributes, tiledb_query = tiledb_array_query(tiledb_array, attrs=attributes)

    for trait in trait_list:
        df = tiledb_query.df[:, trait, :]

        # Create SNP identifier
        df["SNP"] = df["CHR"].astype(str) + ":" + df["POS"].astype(str) + ":" + df["EA"] + ":" + df["NEA"]

        # Safely grab the TRAITID for this dataframe
        if not df.empty and "TRAITID" in df.columns:
            # Drop NaNs just in case, take the first valid one, or fallback to input trait
            valid_ids = df["TRAITID"].dropna()
            if not valid_ids.empty:
                collected_trait_names.append(str(valid_ids.iloc[0]))
            else:
                collected_trait_names.append(str(trait))
        else:
            collected_trait_names.append(str(trait))

        merged_list.append(df)

    # Merge dataframes (Outer Join / Union)
    merged_df = reduce(
        lambda left, right_i: pd.merge(
            left,
            right_i[1].add_suffix(f"_{right_i[0] + 1}").rename(columns={f"SNP_{right_i[0] + 1}": "SNP"}),
            on="SNP",
            how="outer",
        ),
        enumerate(merged_list[1:]),
        merged_list[0],
    )

    variant_names = merged_df["SNP"].values

    # FIX: range() should go up to len(trait_list) to include the last study
    # Study 0 is base, Study 1 is suffix_1, Study 2 is suffix_2, etc.
    effect_sizes = np.column_stack(
        [merged_df["BETA"].values] + [merged_df[f"BETA_{i + 1}"].values for i in range(0, len(trait_list) - 1)]
    )

    standard_error = np.column_stack(
        [merged_df["SE"].values] + [merged_df[f"SE_{i + 1}"].values for i in range(0, len(trait_list) - 1)]
    )

    # Use the robustly collected names instead of querying the merged DF (which has NaNs)
    trait_names = collected_trait_names

    # Remove variants where ALL studies have NaN values
    not_all_nan = ~np.all(np.isnan(effect_sizes), axis=1)
    effect_sizes = effect_sizes[not_all_nan]
    standard_error = standard_error[not_all_nan]
    variant_names = variant_names[not_all_nan]

    # Calculate variance
    variance = standard_error**2

    # Weight effect sizes by inverse variance
    # Note: invalid='ignore' suppresses warnings for NaN division (missing studies)
    with np.errstate(divide="ignore", invalid="ignore"):
        effect_size_divided_by_variance = effect_sizes / variance
        one_divided_by_variance = 1 / variance

    # Sum across studies (NaNs are treated as 0, effectively ignoring missing studies)
    effect_size_divided_by_variance_total = np.nansum(effect_size_divided_by_variance, axis=1)
    one_divided_by_variance_total = np.nansum(one_divided_by_variance, axis=1)

    # Meta-analyzed effect sizes and standard errors
    # Handle potential divide by zero if a row was theoretically all NaN (though filtered above)
    with np.errstate(divide="ignore", invalid="ignore"):
        meta_analysed_effect_sizes = effect_size_divided_by_variance_total / one_divided_by_variance_total
        meta_analysed_standard_error = np.sqrt(1 / one_divided_by_variance_total)

    # Calculate heterogeneity (I²)
    # Broadcast meta_effect to match shape of study effects
    effect_size_deviations_from_mean = np.power(effect_sizes - meta_analysed_effect_sizes[:, np.newaxis], 2)

    # Weighted sum of squared deviations
    effect_size_deviations = np.nansum(one_divided_by_variance * effect_size_deviations_from_mean, axis=1)

    # Degrees of freedom: number of non-NaN studies minus 1
    degrees_of_freedom = np.sum(~np.isnan(effect_sizes), axis=1) - 1

    with np.errstate(divide="ignore", invalid="ignore"):
        i_squared = ((effect_size_deviations - degrees_of_freedom) / effect_size_deviations) * 100

        # Handle cases where I² is NaN or negative
        i_squared[np.isnan(i_squared)] = 0  # Often 0 if Q is close to df, or if only 1 study
        i_squared[i_squared < 0] = 0

    # Calculate meta-analysis p-values
    z_scores = meta_analysed_effect_sizes / meta_analysed_standard_error
    meta_p_values = 2 * (1 - stats.norm.cdf(np.abs(z_scores)))

    # Create results dataframe
    results_df = pd.DataFrame(
        {
            "SNP": variant_names,
            "TRAITID": "_".join(trait_names),
            "BETA": meta_analysed_effect_sizes,
            "SE": meta_analysed_standard_error,
            "P": meta_p_values,
            "I_SQUARED": i_squared,
            "Z_SCORE": z_scores,
        }
    )
    return results_df
