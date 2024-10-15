def process_and_ingest(file_path, uri, checksum_dict, dict_type, renaming_columns, attributes_columns, cfg):
    # Read file with Dask
    df = pd.read_csv(
        file_path,
        compression="gzip",
        sep="\t",
        usecols = attributes_columns
        #usecols=["Chrom", "Pos", "Name", "effectAllele", "Beta", "SE", "ImpMAF"]
    )

    # Rename columns and modify 'chrom' field
    df = df.rename(columns = renaming_columns)
    df["chrom"] = df["chrom"].str.replace('chr', '')
    df["chrom"] = df["chrom"].str.replace('X', '23')
    df["chrom"] = df["chrom"].str.replace('Y', '24')

    # Add trait_id based on the checksum_dict
    file_name = file_path.split('/')[-1]
    df["trait_id"] = checksum_dict[file_name]

    # Store the processed data in TileDB
    tiledb.from_pandas(
        uri=uri,
        dataframe=df,
        index_dims=["chrom", "pos", "trait_id"],
        mode="append",
        column_types=dict_type,
        ctx = ctx
    )