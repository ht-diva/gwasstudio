from typing import List
import pybedtools as pbt


def create_genome_windows(genome: str = "hg19", window: int = 50000000, style: str = "UCSC") -> List[str]:
    """
    Windowed Genome Regions
    Returns a list of bedfile-formatted regions for the given genome.
    :param genome: Genome version (default: hg19)
    :param window: Window size (default: 10000)
    :param style: Chromosome label style format (default: UCSC)
    :return: List of regions
    """
    genome_bins = pbt.BedTool().window_maker(genome=genome, w=window)
    regions_df = genome_bins.to_dataframe()
    if style != "UCSC":
        regions_df.chrom = regions_df.chrom.apply(lambda x: x.lstrip("chr"))
        # convert to 1-based index
    return regions_df.apply(lambda x: f"{x['chrom']}:{x['start'] + 1}-{x['end']}", axis=1).tolist()
