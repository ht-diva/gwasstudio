import json
from pathlib import Path

import cloup
import pandas as pd

help_doc = """
Script to create a JSON dictionary of metadata details from the Decode project files.
The dictionary is used to ingest metadata into a MongoDB collection.
sha256 is the checksum algorithm adopted.
"""


@cloup.command("build_decode_dictionary", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Ingestion options",
    cloup.option(
        "--checksum-file",
        required=True,
        default=None,
        help="Path to the file containing checksums of files to ingest. One per line",
    ),
    cloup.option(
        "--annotation-file", required=True, default=None, help="File path with the annotation for the trait's feature"
    ),
    cloup.option("--output-file", required=True, default=None, help="Path to the JSON output file "),
)
def build_decode_dictionary(checksum_file, annotation_file, output_file):
    input_dictionary = {}
    with open(checksum_file, "r") as fp:
        for line in fp:
            _hash, _, _path = line.strip().split(" ")
            input_dictionary[_path] = _hash

    print("{} records to process".format(len(input_dictionary)))

    project = "DECODE"
    # study = "largescaleplasma2023" to be implemented into the model
    category = "pQTL"
    build = "GRCh38"
    platform = {
        "technology": "proteomics",
        "maker": "Somalogic",
        "model": "SomaScan",
        "version": "v4",
        "normalization": "raw",
    }
    population = "Icelandic"
    tissue = "Plasma"

    annotation_columns = ["SeqId", "UniProt ID", "Ensembl Gene ID"]
    df_annotation = pd.read_csv(Path(annotation_file), usecols=annotation_columns, sep=";")
    renamed_columns = {k: k.replace(" ", "_").lower() for k in annotation_columns}
    df_annotation = df_annotation.rename(columns=renamed_columns)

    result = []

    for path in input_dictionary.keys():
        df = pd.read_csv(Path(path), compression="gzip", nrows=1, usecols=["N"], sep="\t")
        total_samples = int(df.loc[0, "N"])

        file_hash = input_dictionary[path]

        basename = Path(path).name.split("_")[:-1]
        seqid = "-".join([basename[2], basename[3]])
        gene_ids = df_annotation[df_annotation["seqid"] == seqid]["ensembl_gene_id"].item()
        protein_ids = df_annotation[df_annotation["seqid"] == seqid]["uniprot_id"].item()
        trait_desc = {
            "feature": {"seqid": seqid, "gene_ids": gene_ids, "protein_ids": protein_ids},
            "tissue": tissue,
            "platform": platform,
        }

        data = {
            "project": project,
            "data_id": file_hash,
            "category": category,
            "total_samples": total_samples,
            "population": population,
            "build": build,
            "trait_desc": json.dumps(trait_desc),
        }

        if data not in result:
            result.append(data)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    build_decode_dictionary()
