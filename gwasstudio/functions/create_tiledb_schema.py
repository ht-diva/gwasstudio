import tiledb
import numpy as np
def create_tiledb_schema(uri,cfg):
    pos_domain = (1, 3000000000)
    dom = tiledb.Domain(
        tiledb.Dim(name="CHR", dtype=np.bytes_, var=False),
        tiledb.Dim(name="POS", domain = pos_domain, dtype=np.uint32, var=False),
        tiledb.Dim(name="TRAITID", dtype=np.bytes_, var=True))
    schema = tiledb.ArraySchema(
        domain=dom,
        sparse=True,
        allows_duplicates=True,
        attrs=[
            tiledb.Attr(name="BETA", dtype=np.float64, var=False,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)]) ),
            tiledb.Attr(name="SE", dtype=np.float64, var=False,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])),
            tiledb.Attr(name="EAF", dtype=np.float64, var=False,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])),
            tiledb.Attr(name="MLOG10P", dtype=np.float64, var=False,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])),
            tiledb.Attr(name="ALLELE0", dtype=np.bytes_, var=True,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])),
            tiledb.Attr(name="ALLELE1", dtype=np.bytes_, var=True,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])),
            tiledb.Attr(name="SNPID", dtype=np.bytes_, var=True,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)]))
        ]
    )
    tiledb.Array.create(uri, schema, ctx=cfg)