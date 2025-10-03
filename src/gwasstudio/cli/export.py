from pathlib import Path
from typing import Callable

import click
import cloup
import math
import pandas as pd
import tiledb
from dask import delayed, compute

from gwasstudio import logger
from gwasstudio.dask_client import manage_daskcluster, dask_deployment_types
from gwasstudio.methods.extraction_methods import extract_full_stats, extract_regions, extract_snp_list
from gwasstudio.methods.locus_breaker import _process_locusbreaker
from gwasstudio.mongo.models import EnhancedDataProfile
from gwasstudio.utils import check_file_exists, write_table
from gwasstudio.utils.cfg import get_mongo_uri, get_tiledb_config, get_dask_batch_size, get_dask_deployment
from gwasstudio.utils.enums import MetadataEnum
from gwasstudio.utils.metadata import load_search_topics, query_mongo_obj, dataframe_from_mongo_objs
from gwasstudio.utils.mongo_manager import manage_mongo
from gwasstudio.utils.path_joiner import join_path
from dask.distributed import Client

def create_output_prefix_dict(df: pd.DataFrame, output_prefix: str, source_id_column: str) -> dict:
    """
    Generates a dictionary mapping data IDs to output prefixes based on column values.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing the required columns.
        output_prefix (str): Prefix to prepend to output filenames.
        source_id_column (str): Column name containing source id

    Returns:
        dict: Dictionary with 'data_id' as keys and corresponding output prefixes as values.
    """
    logger.debug("Creating output prefix dictionary")
    key_column = "data_id"
    value_column = "output_prefix"

    # Determine the column to use for prefixing
    column_to_get = source_id_column if source_id_column in df.columns else key_column
    logger.debug(f"Selected column for prefixing: {column_to_get}")

    # Construct the output prefix column with fallback
    df[value_column] = f"{output_prefix}_" + df[column_to_get].fillna(df[key_column]).astype(str)

    # Create dictionary mapping data IDs to prefixes
    output_prefix_dict = df.set_index(key_column)[value_column].to_dict()
    logger.debug("Output prefix dictionary created")

    return output_prefix_dict


def _process_function_tasks(
    tiledb_uri: str,
    tiledb_cfg: dict[str, str],
    trait_id_list: list[str],
    attr: str,
    batch_size: int,
    output_prefix_dict: dict[str, str],
    output_format: str,
    *,
    function_name: Callable,
    snp_list_file: str | None = None,
    dask_client: Client = None,
    **kwargs,
) -> None:
    """
    Schedule and execute delayed export tasks.

    Parameters
    ----------
    tiledb_uri : str
        URI of the TileDB array (e.g. ``s3://my-bucket/dataset``).
        The array is opened *inside* each worker, never serialized.
    function_name : Callable
        One of the extraction functions (``extract_full_stats``, …).
    """

    # Helper: read SNP list lazily
    def _read_snp_list(fp: str) -> pd.DataFrame | None:
        if not fp:
            return None
        df = pd.read_csv(fp, usecols=["CHR", "POS"], dtype={"CHR": str, "POS": int})
        return df[df["CHR"].str.isnumeric()]

    # Wrapper that opens the array locally and forwards the call.
    @delayed
    def _run_extraction(
        uri: str,
        cfg: dict[str, str],
        trait: str,
        out_prefix: str | None,
        fmt: str,
        **inner_kwargs,
    ) -> None:
        """Open the TileDB array on the worker and invoke ``function_name``."""
        # Open a *read‑only* handle on the worker.
        #with tiledb.open(uri, mode="r", config=cfg) as arr:
            # ``function_name`` expects the opened array as its first argument.
        #    return function_name(arr, trait, out_prefix, fmt, **inner_kwargs)

        try:
            logger.info(f"[{trait}] Opening TileDB array on worker...")
            with tiledb.open(uri, mode="r", config=cfg) as arr:
                logger.info(f"[{trait}] Running extraction function...")
                result = function_name(arr, trait, out_prefix, fmt, **inner_kwargs)
                logger.info(f"[{trait}] Extraction completed.")
                return result
        except Exception as e:
            logger.error(f"[{trait}] Extraction failed with error: {type(e).__name__}: {e}", exc_info=True)
            raise

    # Prepare kwargs for the downstream extraction routine.
    kwargs["attributes"] = attr.split(",") if attr else None
    if snp_list_file:
        kwargs["snp_list"] = delayed(_read_snp_list)(snp_list_file)

    # Build the delayed tasks – each task receives the URI, not the object.
    tasks: list[delayed] = [
        _run_extraction(
            tiledb_uri,
            tiledb_cfg,
            trait,
            output_prefix_dict.get(trait),
            output_format,
            **kwargs,
        )
        for trait in trait_id_list
    ]

    total_batches = math.ceil(len(tasks) / batch_size)
    # Execute in batches (keeps the Dask graph small).
    #for i in range(0, len(tasks), batch_size):
    #    batch_no = i // batch_size + 1
    #    logger.info(f"Running batch {batch_no}/{total_batches} ({batch_size} items)")
    #    compute(*tasks[i : i + batch_size])
    #    logger.info(f"Batch {batch_no} completed.", flush=True)

    for i in range(0, len(tasks), batch_size):
        batch_no = i // batch_size + 1
        task_batch = tasks[i : i + batch_size]
        trait_ids_in_batch = trait_id_list[i : i + batch_size]
        logger.info(f"Running batch {batch_no}/{total_batches} ({len(task_batch)} items)")
        try:
            logger.debug(f"Traits in batch {batch_no}: {trait_ids_in_batch}")
            if isinstance(dask_client, Client):
                dask_client.run_on_scheduler(
                    lambda dask_scheduler: dask_scheduler.log_event("PING", f"Starting batch {batch_no}")
                )
            compute(*task_batch)
            logger.info(f"Batch {batch_no} completed.", flush=True)
            if isinstance(dask_client, Client):
                dask_client.run_on_scheduler(
                    lambda dask_scheduler: dask_scheduler.log_event("PING", f"Finished batch {batch_no}")
                )
                scheduler_info = dask_client.scheduler_info()
                logger.debug(f"Scheduler workers: {list(scheduler_info.get('workers', {}).keys())}")
        except Exception as e:
            logger.error(
                f"Batch {batch_no} failed with exception: {type(e).__name__}: {e}",
                exc_info=True,
            )
            raise

HELP_DOC = """
Export summary statistics from TileDB datasets with various filtering options.
"""


@cloup.command("export", no_args_is_help=True, help=HELP_DOC)
@cloup.option_group(
    "TileDB options",
    cloup.option("--uri", default="s3://tiledb", help="URI of the TileDB dataset"),
    cloup.option("--output-prefix", default="out", help="Prefix for naming output files"),
    cloup.option(
        "--output-format", type=click.Choice(["parquet", "csv.gz", "csv"]), default="csv.gz", help="Output file format"
    ),
    cloup.option("--search-file", required=True, default=None, help="Input file for querying metadata"),
    cloup.option(
        "--attr",
        required=True,
        default="BETA,SE,EAF,MLOG10P",
        help="string delimited by comma with the attributes to export",
    ),
)
@cloup.option_group(
    "Locusbreaker options",
    cloup.option("--locusbreaker", default=False, is_flag=True, help="Option to run locusbreaker"),
    cloup.option("--pvalue-sig", default=5.0, help="Maximum log p-value threshold within the window"),
    cloup.option("--pvalue-limit", default=3.3, help="Log p-value threshold for loci borders"),
    cloup.option(
        "--hole-size",
        default=250000,
        help="Minimum pair-base distance between SNPs in different loci (default: 250000)",
    ),
)
@cloup.option_group(
    "SNP ID list filtering options",
    cloup.option(
        "--snp-list-file",
        default=None,
        help="A txt file which must include CHR and POS columns",
    ),
)
@cloup.option_group(
    "Option to plot results",
    cloup.option(
        "--plot-out",
        default=False,
        is_flag=True,
        help="Boolean to plot results. If enabled, the output will be plotted as a Manhattan plot.",
    ),
    cloup.option(
        "--color-thr",
        default="red",
        help="Color for the points passing the threshold line in the plot (default: red)",
    ),
    cloup.option(
        "--s-value",
        default=5,
        help="Value for the suggestive p-value line in the plot (default: 5)",
    ),
)
@cloup.option_group(
    "Regions filtering options",
    cloup.option(
        "--get-regions",
        default=None,
        help="Bed file with regions to filter",
    ),
    cloup.option(
        "--maf",
        default=0.01,
        help="MAF filter to apply to each region",
    ),
    cloup.option(
        "--phenovar",
        default=False,
        is_flag=True,
        help="Boolean to compute phenovariance (Work in progress, not fully implemented yet)",
    ),
    cloup.option(
        "--nest",
        default=False,
        is_flag=True,
        help="Estimate effective population size (Work in progress, not fully implemented yet)",
    ),
)
@click.pass_context
def export(
    ctx: click.Context,
    uri: str,
    search_file: str,
    attr: str,
    output_prefix: str,
    output_format: str,
    pvalue_sig: float,
    pvalue_limit: float,
    hole_size: int,
    phenovar: bool,
    nest: bool,
    maf: float,
    snp_list_file: str | None,
    locusbreaker: bool,
    get_regions: str | None,
    plot_out: bool,
    color_thr: str,
    s_value: int,
) -> None:
    """Export summary statistics based on selected options."""
    cfg = get_tiledb_config(ctx)
    if not check_file_exists(search_file, logger):
        exit(1)

    search_topics, output_fields = load_search_topics(search_file)
    if plot_out:
        # plot_config = get_plot_config(ctx)
        # if not plot_config:
        # logger.error("Plotting configuration is required for plotting output.")
        # exit(1)
        if "data_ids" not in search_topics:
            logger.error("Plotting option is enabled but no data_ids is provided in the search file.")
            exit(1)
        if len(search_topics["data_ids"]) > 20:
            logger.error(
                "Plotting option is enabled but too many data_ids are provided in the search file. Please limit to 20 data_ids."
            )
            exit(1)
    # Query MongoDB
    with manage_mongo(ctx):
        mongo_uri = get_mongo_uri(ctx)
        obj = EnhancedDataProfile(uri=mongo_uri)
        objs = query_mongo_obj(search_topics, obj)

    df = dataframe_from_mongo_objs(output_fields, objs)

    # Write metadata query result
    path = Path(output_prefix)
    output_path = path.with_suffix("").with_name(path.stem + "_meta")
    kwargs = {"index": False}
    log_msg = f"{len(objs)} results found. Writing to {output_path}.csv"
    write_table(df, str(output_path), logger, file_format="csv", log_msg=log_msg, **kwargs)

    # Create an output prefix dictionary to generate output filenames
    source_id_column = MetadataEnum.get_source_id_field()
    output_prefix_dict = create_output_prefix_dict(df, output_prefix, source_id_column=source_id_column)

    # Process according to selected options
    if get_dask_deployment(ctx) not in dask_deployment_types:
        logger.error(f"A valid dask deployment type must be set from: {dask_deployment_types}")
        raise SystemExit(1)

    with manage_daskcluster(ctx) as client:
        batch_size = get_dask_batch_size(ctx)
        grouped = df.groupby(MetadataEnum.get_tiledb_grouping_fields(), observed=False)
        for name, group in grouped:
            group_name = "_".join(name)
            logger.info(f"Processing the group {group_name}")
            trait_ids = list(group["data_id"])
            tiledb_uri = join_path(uri, group_name)
            logger.debug(f"tiledb_uri: {tiledb_uri}")

            # Common argument list
            common_args = [
                tiledb_uri,  # <-- URI, not an opened array
                cfg,
                trait_ids,
                attr,
                batch_size,
                output_prefix_dict,
                output_format,
            ]

            # Dispatch the appropriate extraction routine
            match (locusbreaker, snp_list_file, get_regions):
                case (True, _, _):
                    _process_function_tasks(
                        *common_args,
                        function_name=_process_locusbreaker,
                        maf=maf,
                        hole_size=hole_size,
                        pvalue_sig=pvalue_sig,
                        pvalue_limit=pvalue_limit,
                        phenovar=phenovar,
                    )
                case (_, str() as snp_fp, _):
                    _process_function_tasks(
                        *common_args,
                        function_name=extract_snp_list,
                        snp_list_file=snp_fp,
                        plot_out=plot_out,
                        color_thr=color_thr,
                        s_value=s_value,
                        dask_client=client,
                    )
                case (_, _, str() as bed_fp):
                    bed_region = pd.read_csv(
                        bed_fp,
                        sep="\t",
                        header=None,
                        names=["CHR", "START", "END"],
                    )
                    bed_region["CHR"] = bed_region["CHR"].astype(int)
                    _process_function_tasks(
                        *common_args,
                        function_name=extract_regions,
                        bed_region=bed_region.groupby("CHR"),
                        plot_out=plot_out,
                        color_thr=color_thr,
                        s_value=s_value,
                    )
                case _:
                    _process_function_tasks(
                        *common_args,
                        function_name=extract_full_stats,
                        plot_out=plot_out,
                        color_thr=color_thr,
                        s_value=s_value,
                    )
