import pandas as pd
import tiledb

def process_and_ingest(file_path, uri, checksum_dict, cfg):
    # Read file with Pandas
    df = pd.read_csv(
        file_path,
        compression="gzip",
        sep="\t",
        usecols=["CHR", "POS", "SNPID", "EAF", "EA", "NEA", "BETA", "SE", "MLOGP10"]
        dtype = {"CHR":str, "POS":np.uint32, "SNPID":str, "EAF":np.float32, "EA":str, "NEA":str, "BETA":np.float32, "SE":np.float32, "MLOGP10":np.float32}
    )
    # Add trait ID
    df["TRAITID"] = checksum_dict[file_path]
    ctx = tiledb.Ctx(cfg)
    # Store the processed data in TileDB
    tiledb.from_pandas(
        uri=uri, dataframe=df, index_dims=["CHR", "POS", "TRAITID"], mode="append", ctx=ctx
    )
