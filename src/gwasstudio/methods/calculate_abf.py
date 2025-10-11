import numpy as np
def _calculate_abf(
        beta:float,
        se:float,
        sdY:float
    ):
    """ Modifiied version of the Approximate Bayes Factor (Wakefield, 2009, Genet Epidemiol.).
        Based on code from coloc: https://github.com/chr1swallace/coloc
        Differently from the original here we always assume to have beta and se so we don't have to re-calculate it.
    Args:
        beta (float): GWAS beta
        se (float): GWAS se
        sdY: Phenotypic variance for the trait (only considered for quantitatives)
        prop_cases (float or None): number of cases, if left blank will assume
            quantitative trait
    Returns:
        natural log(ABF)
    """
    # Assert/set types
    beta = float(pval)
    se = float(se)
    prop_cases = float(prop_cases) if prop_cases else None

    if prop_cases is None:
        sd_prior = 0.2
        
    else:
        sd_prior = 0.15 * sdY
    v = se**2
    # Calculate Z-score
    z = beta/se

    # Calc shrinkage factor: ratio of the prior variance to the total variance
    r = sd_prior**2 / (sd_prior**2 + v)

    # Approximate BF - ln scale to compare in log natural scale with LR diff
    lABF = 0.5 * (np.log(1 - r) + (r * z**2))

    return lABF
