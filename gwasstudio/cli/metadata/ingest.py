import json
from pathlib import Path

import cloup
import pandas as pd

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
        "--filepath",
        default=None,
        help="Path to the file to ingest",
    ),
    cloup.option(
        "--pathlist",
        default=None,
        help="Path to the file containing a list of files to ingest. One path per line",
    ),
    cloup.option("--annotation-file", default=None, help="File path with the annotation for the trait's feature"),
)
def meta_ingest(filepath, pathlist, annotation_file):
    input_file_list = []
    if filepath:
        input_file_list.append(filepath)
    elif pathlist:
        with open(pathlist, "r") as fp:
            for line in fp:
                input_file_list.append(line.strip())
    else:
        logger.error("No input provided")
        exit()

    # file_list = sorted(Path(data_path).rglob("*.txt.gz"))
    project = "DECODE"
    # study = "largescaleplasma2023"
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
    tissue = "Plasma"
    annotation_columns = ["SeqId", "UniProt ID", "Ensembl Gene ID"]
    df_annotation = pd.read_csv(Path(annotation_file), usecols=annotation_columns, sep=";")
    renamed_columns = {k: k.replace(" ", "_").lower() for k in annotation_columns}
    df_annotation = df_annotation.rename(columns=renamed_columns)

    logger.info("{} documents to ingest".format(len(input_file_list)))
    for path in input_file_list:
        df = pd.read_csv(Path(path), compression="gzip", nrows=1, usecols=["N"], sep="\t")
        total_samples = int(df.loc[0, "N"])
        file_hash = compute_sha256(fpath=path)
        basename = Path(path).name.split("_")[:-1]
        seqid = "-".join([basename[2], basename[3]])
        gene_ids = df_annotation[df_annotation["seqid"] == seqid]["ensembl_gene_id"].item()
        protein_ids = df_annotation[df_annotation["seqid"] == seqid]["uniprot_id"].item()
        trait_desc = {
            "feature": {"seqid": seqid, "gene_ids": gene_ids, "protein_ids": protein_ids},
            "tissue": tissue,
            "platform": platform,
        }

        kwargs = {
            "project": project,
            "data_id": file_hash,
            "category": category,
            "total_samples": total_samples,
            "population": population,
            "build": build,
            "trait_desc": json.dumps(trait_desc),
        }
        logger.debug(kwargs)
        obj = EnhancedDataProfile(**kwargs)
        obj.save()
