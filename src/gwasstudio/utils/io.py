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


# Helper: check valid allele ordering
def _validate_alleles(EA: str, NEA: str):
    if len(EA) == len(NEA):
        if EA > NEA:
            raise ValueError("SNPs must be alphabetically ordered (EA < NEA).")
    else:
        if len(EA) < len(NEA):
            raise ValueError("EA must be the longer allele in indels.")


# Helper: read trait and SNP list
def read_trait_snps(fp: str) -> pd.DataFrame | None:
    if not fp:
        return None
    try:
        df = pd.read_csv(
            fp,
            sep=",",
            header=0,
            usecols=["SOURCE_ID", "CHR", "POS", "EA", "NEA"],
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

        # Check if alleles are valid (alphabetically ordered)
        invalid_rows = []
        for idx, row in df.iterrows():
            try:
                _validate_alleles(row["EA"], row["NEA"])
            except ValueError:
                invalid_rows.append(idx)
        if invalid_rows:
            raise ValueError("Invalid allele ordering detected."
                             f"Examples of invalid rows:\n{df.loc[invalid_rows].head()}")
        return df
    except Exception:
        raise ValueError(f"--get-regions-leadsnps file '{fp}' should have the format SOURCE_ID,CHR,POS,EA,NEA")
