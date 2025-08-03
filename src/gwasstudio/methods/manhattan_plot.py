import dash_bio
import plotly.io as pio
import pandas as pd

def _plot_manhattan(locus: pd.DataFrame,  
                    title_plot: str, 
                    out: str,
                    color_thr: str = 'red'):
    """
    Create a Manhattan plot from a numpy array and save it to a file.

    Args:
        locus (pd.DataFrame): DataFrame containing the data for the Manhattan plot.
        title_plot (str): Title of the plot.
        out (str): Output file path to save the HTML plot.
        color_thr (str): Color for the points passing the threshold line. Default is 'red'.

    Returns:
        None: Saves the plot to the specified file.
    """
    if locus.empty or len(locus) < 40:
        raise ValueError("Input DataFrame is empty or smaller than 40 variants. Cannot create Manhattan plot.")
    
    # Ensure required columns are present
    required_columns = {"CHR", "POS", "MLOG10P"}
    if not required_columns.issubset(locus.columns):
        raise ValueError(f"Input DataFrame must contain columns: {required_columns}")
    
    # Create SNP identifier
    locus["SNP"] = locus["CHR"].astype(str) + ':' + locus["POS"].astype(str)
    locus = locus.reset_index(drop=True)
    # Generate Manhattan plot
    try:
        fig = dash_bio.ManhattanPlot(
            dataframe=locus,
            title="test",
            chrm='CHR',
            bp='POS',
            p='MLOG10P',
            #annotation='CHR',
            gene='CHR',
            logp=False,
            highlight_color="red"
        )
        # Save the plot to an HTML file
        pio.write_html(fig, file='test.html', auto_open=False)
    except ValueError as e:
        # Catch and re-raise errors with additional context
        raise ValueError(f"An error occurred while generating the Manhattan plot: {e}")
