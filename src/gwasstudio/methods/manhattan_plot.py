import pandas as pd


def _plot_manhattan(locus: pd.DataFrame, title_plot: str, out: str, color_thr: str = "red", s_value: int = 5) -> None:
    """
    Create a Manhattan plot from a numpy array and save it to a file.

    Args:
        locus (pd.DataFrame): DataFrame containing the data for the Manhattan plot.
        title_plot (str): Title of the plot.
        out (str): Output file path to save the HTML plot.
        color_thr (str): Color for the points passing the threshold line. Default is 'red'.
        s_value (int): Value for the suggestive p-value line in the plot. Default is 5.

    Returns:
        None: Saves the plot to the specified file.
    """
    # Check if dash_bio is imported
    try:
        import dash_bio
    except ImportError:
        raise ImportError(
            "The 'dash_bio' library is not installed. Please install it using 'make dependencies_extras'."
        )

    # Check if plotly is imported
    try:
        import plotly.io as pio
    except ImportError:
        raise ImportError("The 'plotly' library is not installed. Please install it using 'make dependencies_extras'.")

    if locus.empty or len(locus) < 40:
        raise ValueError("Input DataFrame is empty or smaller than 40 variants. Cannot create Manhattan plot.")

    # Ensure required columns are present
    required_columns = {"CHR", "POS", "MLOG10P"}
    if not required_columns.issubset(locus.columns):
        raise ValueError(f"Input DataFrame must contain columns: {required_columns}")

    # Create SNP identifier
    locus["SNP"] = locus["CHR"].astype(str) + ":" + locus["POS"].astype(str)
    locus = locus.reset_index(drop=True)
    # Generate Manhattan plot
    try:
        fig = dash_bio.ManhattanPlot(
            dataframe=locus,
            title=title_plot,
            chrm="CHR",
            bp="POS",
            p="MLOG10P",
            # annotation='CHR',
            gene="CHR",
            logp=False,
            highlight_color=color_thr,
            suggestiveline_value=s_value,
        )
        # Save the plot to an HTML file
        pio.write_html(fig, file=f"{out}.html", auto_open=False)
    except ValueError as e:
        # Catch and re-raise errors with additional context
        raise ValueError(f"An error occurred while generating the Manhattan plot: {e}")
