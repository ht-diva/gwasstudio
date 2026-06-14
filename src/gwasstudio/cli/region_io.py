from io import StringIO
from pathlib import Path

import pandas as pd

from gwasstudio import logger


# Helper: format chromosome
def _clean_chr(df: pd.DataFrame, logger) -> pd.DataFrame:

    # Remove 'chr' prefix and convert X/Y to 23/24
    df.loc[:, "CHR"] = df["CHR"].astype(str).str.replace("chr", "", case=False).replace({"X": "23", "Y": "24"})

    count_row_before = df.shape[0]
    df = df[df["CHR"].str.isnumeric()]
    row_diff = count_row_before - df.shape[0]
    if row_diff > 0:
        logger.warning(f"Removed {row_diff} rows with non-numeric CHR values.")

    df.loc[:, "CHR"] = df["CHR"].astype(int)

    return df


# Helper: whether input is an existing path or not
def _is_path(fp: str) -> bool:
    try:
        return Path(fp).exists()
    except (OSError, ValueError):
        return False


# Helper: check correct format for inline string input
# 2 columns -> SNP format (CHR,POS;CHR,POS)
# 3 columns -> BED format (CHR,START,END;CHR,START,END)
def _validate_string(fp: str) -> int:
    expected_ncols = None

    records = fp.split(";")
    for record in records:
        fields = record.split(",")
        if any(not f.strip() for f in fields):
            raise ValueError(f"Empty field in record: '{record}'")
        ncols = len(fields)
        if ncols not in (2, 3):
            raise ValueError(f"Invalid record '{record}'. Expected CHR,POS or CHR,START,END")
        if expected_ncols is None:
            expected_ncols = ncols
        elif ncols != expected_ncols:
            raise ValueError(f"Mixed inline in record '{record}'. Use either CHR,POS or CHR,START,END consistently.")

    return expected_ncols


# Helper: read BED region file or SNP list as:
# 1) a file path
# 2) an inline string
def read_to_bed(fp: str) -> pd.DataFrame | None:
    if not fp:
        return None

    # File path input
    if _is_path(fp):
        try:
            # BED format
            df = pd.read_csv(
                fp,
                sep="\t",
                header=None,
                names=["CHR", "START", "END"],
                usecols=range(3),
                dtype={"CHR": str, "START": int, "END": int},
            )
            df = _clean_chr(df, logger)

            return df
        except Exception as e:
            logger.debug(f"Trying BED format failed: {e}")

        try:
            # SNP list
            df = pd.read_csv(
                fp,
                usecols=["CHR", "POS"],
                dtype={"CHR": str, "POS": int},
            )
            df = _clean_chr(df, logger)

            df = df.rename(columns={"POS": "START"})
            df.loc[:, "END"] = df["START"] + 1

            return df
        except Exception as e:
            logger.debug(f"Trying SNP list format failed: {e}")

            raise ValueError(
                f"--get_regions_snps file '{fp}' should be in BED format or SNP list format (CHR,POS)"
            ) from e

    # Inline string input
    else:
        try:
            # Check inline string format
            ncols = _validate_string(fp)

            # Extract content from valid string
            str_content = fp.replace(";", "\n")

            # BED-like inline format
            if ncols == 3:
                df = pd.read_csv(
                    StringIO(str_content),
                    sep=",",
                    header=None,
                    names=["CHR", "START", "END"],
                    dtype={"CHR": str, "START": int, "END": int},
                )
                df = _clean_chr(df, logger)

                return df

            # SNP-like inline format
            elif ncols == 2:
                df = pd.read_csv(
                    StringIO(str_content),
                    sep=",",
                    header=None,
                    names=["CHR", "POS"],
                    dtype={"CHR": str, "POS": int},
                )
                df = _clean_chr(df, logger)

                df = df.rename(columns={"POS": "START"})
                df["END"] = df["START"] + 1

                return df

        except ValueError:
            raise

        # Wrong input format
        except Exception as e:
            raise ValueError(
                "--get_regions_snps should be either:\n"
                "1. A valid BED file\n"
                "2. A valid SNP list file\n"
                "3. An inline string:\n"
                "   CHR,START,END;CHR,START,END\n"
                "   or\n"
                "   CHR,POS;CHR,POS"
            ) from e


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
        df = _clean_chr(df, logger)

        # Check if alleles are valid (alphabetically ordered)
        invalid_rows = []
        for idx, row in df.iterrows():
            try:
                _validate_alleles(row["EA"], row["NEA"])
            except ValueError:
                invalid_rows.append(idx)
        if invalid_rows:
            raise ValueError(
                f"Invalid allele ordering detected.Examples of invalid rows:\n{df.loc[invalid_rows].head()}"
            )
        return df
    except Exception:
        raise ValueError(f"--get-regions-leadsnps file '{fp}' should have the format SOURCE_ID,CHR,POS,EA,NEA")
