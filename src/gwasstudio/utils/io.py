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


# Helper: read trait and SNP list
def read_trait_snps(fp: str) -> pd.DataFrame | None:
    if not fp:
        return None
    try:
        df = pd.read_csv(
            fp,
            sep=",",
            header=0,
            names=["SOURCE_ID", "CHR", "POS", "EA", "NEA"],
            usecols=range(5),
            dtype={"SOURCE_ID": str, "CHR": str, "POS": int, "EA": str, "NEA": str},
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

        # Check if alleles are alphabetically ordered
        alleles_disordered = df[df["EA"] >= df["NEA"]]
        if not alleles_disordered.empty:
            raise ValueError(
                "Alleles are not alphabetically ordered (EA must < NEA). "
                f"Examples of invalid rows:\n{alleles_disordered.head()}"
            )

        return df
    except Exception:
        raise ValueError(f"--get-regions-leadsnps file '{fp}' should have the format SOURCE_ID,CHR,POS,EA,NEA")
