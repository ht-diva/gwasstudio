import dash_bio
import plotly.io as pio
import pandas as pd

def _plot_manhattan(      locus: pd.DataFrame,  
                          title_plot: str, 
                          out: str,
                          chrom: str = 'CHR',
                          pos: str = 'BP',
                          pval: str = 'P',
                          color_thr: str = 'red'
                          ):
    """
    Create a Manhattan plot from a numpy array and save it to a file.

    Args:
        locus (pd.DataFrame): DataFrame containing the data for the Manhattan plot.
        title (str): Title of the plot.
        out (str): Output file path to save the html plot.
        chrom (str): Column name for chromosome data. Default is 'CHR'.
        pos (str): Column name for position data. Default is 'BP'.
        pval (str): Column name for p-value data. Default is 'P'.
        color_thr (str): Color for the points passing the threshold line. Default is 'red'.
        annotation (str): Column name for annotation data. Default is "STUDY_ID".

    Returns: File path of the saved plot.
    """

    fig = dash_bio.ManhattanPlot(
    dataframe=locus,
    title=title_plot,
    chrom ='CHR',
    pos = 'BP',
    pval = 'P',
    annotation='CHR',
    logp = False,
    highlight_color=color_thr
    )
    pio.write_html(fig, file=f'{out}.html', auto_open=False)
