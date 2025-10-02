import numpy as np

def _meta_analyse(dataframe, variant_names, phenotype_names):
        """
        Meta analysis for all associations.

        For each association, we apply an inverse variance meta analysis.

        For combining the results between cohorts, we complete the matrices
        with all missing values for ease of use.
        :param chromosomes:
        :param variant_names:
        :param phenotype_names:

        """
        # We rely on the methods below to return a 3d matrix wherein the
        # 0th dimension represents the studies / cohorts, the
        # 1st dimension represents the variants, and the
        # 2nd dimension represents the phenotypes.
        effect_sizes = np.stack([cohort.analyser.get_betas() for cohort in dataframe.cohort_list])
        standard_error = np.stack([cohort.analyser.standard_error for cohort in dataframe.cohort_list])

        # Missing values are expected to be NaN
        # We should check which phenotype, variant combinations are NaN in all
        # cohorts. Then we can remove these from the variant and phenotype names
        not_all_nan = ~np.all(np.isnan(effect_sizes), axis=0)
        effect_sizes = effect_sizes[:, not_all_nan]
        standard_error = standard_error[:, not_all_nan]

        # Now create the cartesian product of all variant, phenotype names
        test_combinations = np.vstack(
            (np.repeat(variant_names, phenotype_names.shape[0]),
             np.tile(phenotype_names, variant_names.shape[0]))).T

        # From these combinations we can remove all that are nan in all cohorts
        test_combinations = test_combinations[not_all_nan.flatten()]

        # The inverse variance weighted meta-analysis requires calculating
        # the variance of the effect estimate. We do this by calculating the
        # 2nd power of the standard error.
        variance = standard_error ** 2

        # Weight the effect sizes by dividing the effect size by
        # the variance of the effect estimate.
        # This is the same thing as multiplying the effect size by
        # (1 / variance).
        effect_size_divided_by_variance = effect_sizes / variance
        effect_size_divided_by_variance_total = np.nansum(
            effect_size_divided_by_variance, axis=0)

        # Calculate the total weight as the inverse of the variance of the effect
        # estimate.
        one_divided_by_variance = 1 / variance
        one_divided_by_variance_total = np.nansum(one_divided_by_variance, axis=0)

        # The total weighted effect size should be divided still by the
        # total weight of each of the studies.
        meta_analysed_effect_sizes = effect_size_divided_by_variance_total / one_divided_by_variance_total
        meta_analysed_standard_error = np.sqrt(1 / one_divided_by_variance_total)

        # We need to quantify heterogeneity. We do this by calculating I2 values.
        # The first step is calculating a Q-value:
        # The weighted sum of squared differences between individual study
        # effects and the pooled effects across studies.
        effect_size_deviations_from_mean = np.power(
            effect_sizes - meta_analysed_effect_sizes, 2)

        # Weight and sum all effect size deviations from the mean.
        effect_size_deviations = np.nansum(
            one_divided_by_variance * effect_size_deviations_from_mean,
            axis=0)

        # Expected effect size deviations from the mean
        degrees_of_freedom = (np.sum(~np.isnan(effect_size_deviations_from_mean), axis=0) - 1)
        with np.errstate(divide='ignore'):
            i_squared = (
                ((effect_size_deviations - degrees_of_freedom)
                 / effect_size_deviations
                 ) * 100)

            # If the i_squared happens to be NaN, set it to a hundred.
            # Lack of precision causing the effect_size_deviations to be slightly different from 0
            # make the I_squared also one hundred.
            i_squared[np.isnan(i_squared)] = 100

        meta_analysis_sample_sizes = np.nansum(
            dataframe.get_sample_sizes(), axis=0)[not_all_nan]

        dataframe.set_results(meta_analysed_effect_sizes,
                         meta_analysed_standard_error,
                         meta_analysis_sample_sizes,
                         i_squared,
                         test_combinations)