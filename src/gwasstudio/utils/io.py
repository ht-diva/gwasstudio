import pandas as pd

from gwasstudio import logger


# Helper: read BED region file or SNP list
def read_to_bed(fp: str) -> pd.DataFrame | None:
    if not fp:
        return None
    try:
        # Try BED format
        df = pd.read_csv(
            fp,
            sep="\t",
            header=None,
            names=["CHR", "START", "END"],
            usecols=range(3),
            dtype={"CHR": str, "START": int, "END": int},
        )

        # Remove 'chr' prefix and convert X/Y to 23/24
        df.loc[:, "CHR"] = df["CHR"].str.replace("chr", "", case=False)
        df.loc[:, "CHR"] = df["CHR"].replace({"X": "23", "Y": "24"})

        count_row_before = df.shape[0]
        df = df[df["CHR"].str.isnumeric()]
        row_diff = count_row_before - df.shape[0]
        if row_diff > 0:
            logger.warning(f"Removed {row_diff} rows with non-numeric CHR values.")

        df.loc[:, "CHR"] = df["CHR"].astype(int)

        return df
    except Exception as e:
        logger.debug(f"Trying to use BED format: {e}")
        pass
    try:
        # Try SNP list and convert to BED format
        df = pd.read_csv(fp, usecols=["CHR", "POS"], dtype={"CHR": str, "POS": int})

        # Remove 'chr' prefix and convert X/Y to 23/24
        df.loc[:, "CHR"] = df["CHR"].str.replace("chr", "", case=False)
        df.loc[:, "CHR"] = df["CHR"].replace({"X": "23", "Y": "24"})

        count_row_before = df.shape[0]
        df = df[df["CHR"].str.isnumeric()]
        row_diff = count_row_before - df.shape[0]
        if row_diff > 0:
            logger.warning(f"Removed {row_diff} rows with non-numeric CHR values.")

        df.loc[:, "CHR"] = df["CHR"].astype(int)
        df = df.rename(columns={"POS": "START"})
        df.loc[:, "END"] = df["START"] + 1

        return df
    except Exception as e:
        logger.debug(f"Trying to use SNP list format: {e}")
        raise ValueError(f"--get_regions_snps file '{fp}' should be in BED format or a SNP list (CHR,POS)")
