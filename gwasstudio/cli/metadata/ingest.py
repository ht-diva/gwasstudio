import json
from pathlib import Path

import cloup

from gwasstudio import logger
from gwasstudio.mongo.models import EnhancedDataProfile
from gwasstudio.utils import compute_sha256

help_doc = """
Ingest metadata into a MongoDB collection.
"""


@cloup.command("meta_ingest", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Ingestion options",
    cloup.option(
        "--data-path",
        default=None,
        help="Path to dataset directory",
    ),
)
def meta_ingest(data_path):
    file_list = sorted(Path(data_path).rglob("*.txt.gz"))
    project = "DECODE_largescaleplasma2023"
    population = "Icelandic"
    category = "pQTL"
    build = "GRCh38"
    platform = {
        "technology": "proteomics",
        "maker": "Somalogic",
        "model": "SomaScan",
        "version": "v4",
        "normalization": "raw",
    }
    logger.info("{} documents to ingest".format(len(file_list)))
    for path in file_list:
        file_hash = compute_sha256(fpath=path)
        basename = Path(path).name.split("_")[:-1]
        seqid = "_".join([basename[2], basename[3]])
        genid = basename[4]
        protid = "_".join(basename[5:])
        tissue = "Plasma"
        trait_desc = {
            "feature": {"seqid": seqid, "genid": genid, "protid": protid},
            "tissue": tissue,
            "platform": platform,
        }

        kwargs = {
            "project": project,
            "data_id": file_hash,
            "category": category,
            "population": population,
            "build": build,
            "trait_desc": json.dumps(trait_desc),
        }
        logger.debug(kwargs)
        obj = EnhancedDataProfile(**kwargs)
        obj.save()
