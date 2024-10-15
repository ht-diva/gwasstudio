import tiledb
import numpy as np
import pandas as pd
dict_type = {"chrom":np.uint8, "pos":np.uint32, "trait_id":np.dtype('S64'), "beta":np.float32, "se":np.float32, "freq":np.float32, "alt":np.dtype('S5'), "SNP":np.dtype('S20')}
# Define the TileDB array schema with SNP, gene, and population dimensions
def create_tiledb_schema(uri,cfg):
    chrom_domain = (1, 22)
    pos_domain = (1, 3000000000)
    dom = tiledb.Domain(
        tiledb.Dim(name="chrom", domain = chrom_domain,  dtype=np.uint8, var=False),
        tiledb.Dim(name="pos", domain = pos_domain, dtype=np.uint32, var=False),
        tiledb.Dim(name="trait_id", dtype=np.dtype('S64'), var=True)
    )
    schema = tiledb.ArraySchema(
        domain=dom,
        sparse=True,
        allows_duplicates=True,
        attrs=[
            tiledb.Attr(name="beta", dtype=np.float32, var=False,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)]) ),
            tiledb.Attr(name="se", dtype=np.float32, var=False,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])),
            tiledb.Attr(name="freq", dtype=np.float32, var=False,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])),
            tiledb.Attr(name="alt", dtype=np.dtype('S5'), var=True,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])),
            tiledb.Attr(name="SNP", dtype=np.dtype('S20'), var=True,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)]))
        ]
    )
    tiledb.Array.create(uri, schema, ctx=cfg)