def genome_windows(genome, window=10000, style="UCSC"):
    """
    Windowed Genome Regions
    Returns a list of bedfile-formatted regions for the given genome.
    :param genome: Genome version (default: hg19)
    :param window: Window size (default: 10000)
    :param style: Chromosome label style format (default: UCSC)
    """
    import pybedtools as pbt
    import pandas as pd
    genome_bins = pbt.BedTool().window_maker(genome=genome, w=window)
    regions_df = genome_bins.to_dataframe()
    # modify contig naming scheme to match the dataset
    return regions_df.apply(lambda x: f"{x['chrom']}:{x['start'] + 1}-{x['end']}", axis=1).tolist()
