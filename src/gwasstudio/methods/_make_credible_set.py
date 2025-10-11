import numpy as np
def calc_credible_sets_standard(data, confidence_levels=[0.95, 0.99], pp_threshold=0.01):
    """
    Standard approach for credible set calculation
    """
    data = data.sort_values("logABF", ascending=False).copy()
    
    # Calculate posterior probabilities
    logABF = data["logABF"].values
    max_logABF = logABF.max()
    relative_probs = np.exp(logABF - max_logABF)
    postprob = relative_probs / relative_probs.sum()
    
    data["postprob"] = postprob
    data["postprob_cumsum"] = postprob.cumsum()
    
    # Create credible sets for each confidence level
    for level in confidence_levels:
        cred_set_name = f"is{int(level*100)}_credset"
        # Include all SNPs until cumulative probability >= level
        data[cred_set_name] = data["postprob_cumsum"] <= level
        
        # If no SNPs reach the threshold, include at least the top one
        if not data[cred_set_name].any():
            data.loc[data.index[0], cred_set_name] = True
    
    # Filter and return
    cred_cols = [f"is{int(level*100)}_credset" for level in confidence_levels]
    in_any_credset = data[cred_cols].any(axis=1)
    
    cred_set_res = data[in_any_credset & (data["postprob"] > pp_threshold)].copy()
    
    return cred_set_res