import math
from pathlib import Path
from typing import Callable, List, Union

import click
import cloup
import pandas as pd
import tiledb
from dask import delayed, compute
from dask.distributed import Client

from gwasstudio import logger
from gwasstudio.dask_client import manage_daskcluster, dask_deployment_types
from gwasstudio.methods.extraction_methods import extract_full_stats, extract_regions_snps
from gwasstudio.methods.locus_breaker import _process_locusbreaker
from gwasstudio.methods.meta_analysis import _meta_analysis
from gwasstudio.mongo.models import EnhancedDataProfile
from gwasstudio.utils import check_file_exists, write_table
from gwasstudio.utils.cfg import get_mongo_uri, get_tiledb_config, get_dask_batch_size, get_dask_deployment
from gwasstudio.utils.enums import MetadataEnum
from gwasstudio.utils.io import read_to_bed
from gwasstudio.utils.metadata import load_search_topics, query_mongo_obj, dataframe_from_mongo_objs
from gwasstudio.utils.mongo_manager import manage_mongo
from gwasstudio.utils.path_joiner import join_path


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
    group: pd.DataFrame,
    attr: str,
    batch_size: int,
    output_prefix_dict: dict[str, str],
    output_format: str,
    *,
    function_name: Callable,
    regions_snps: str | None = None,
    dask_client: Client = None,
    output_prefix=None,
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
    # Check Dask client
    if dask_client is None:
        raise ValueError("Missing Dask client")

    # Wrapper that opens the array locally and forwards the call.
    def _run_extraction(
        uri: str,
        cfg: dict[str, str],
        traits: Union[str, List[str]],
        out_prefix: str | None,
        **inner_kwargs,
    ) -> pd.DataFrame:
        """Open the TileDB array on the worker and invoke ``function_name``."""
        # Open a *read‑only* handle on the worker.
        with tiledb.open(uri, mode="r", config=cfg) as arr:
            # ``function_name`` expects the opened array as its first argument.
            return function_name(arr, traits, out_prefix, **inner_kwargs)

    def _run_transformation(gwas_df: pd.DataFrame, meta_df: pd.DataFrame, trait_id: str) -> pd.DataFrame:
        #  Optional metadata broadcast – only used when ``skip_meta`` is False.
        if isinstance(group, pd.Series):
            return gwas_df

        id_col = "data_id"
        meta_row = meta_df.loc[meta_df[id_col] == trait_id].squeeze()
        # meta_dict = meta_row.squeeze().to_dict()
        meta_dict = {f"meta_{k}": v for k, v in meta_row.drop(["data_id", "output_prefix"]).to_dict().items()}

        broadcast = {col: [val] * len(gwas_df) for col, val in meta_dict.items()}
        return gwas_df.assign(**broadcast)

    # Prepare kwargs for the downstream extraction routine.
    kwargs["attributes"] = attr.split(",") if attr else None

    if regions_snps:
        kwargs["regions_snps"] = delayed(read_to_bed)(regions_snps)

    trait_id_list = group["data_id"].tolist() if not isinstance(group, pd.Series) else group.tolist()
    # Build the delayed tasks – each task receives the URI, not the object.
    tasks = []
    if function_name.__name__ == "_process_locusbreaker":
        # Locusbreaker returns a tuple (segments, intervals).
        for trait in trait_id_list:
            delayed_tuple = delayed(_run_extraction)(
                tiledb_uri,
                tiledb_cfg,
                trait,
                None,
                **kwargs,
            )
            # Extract the two DataFrames lazily.
            seg = delayed(lambda t: t[0])(delayed_tuple)
            intv = delayed(lambda t: t[1])(delayed_tuple)

            # write each DataFrame (still delayed)
            seg_task = delayed(write_table)(
                seg,
                f"{output_prefix_dict.get(trait)}_segments",
                logger,
                output_format,
                index=False,
            )
            int_task = delayed(write_table)(
                intv,
                f"{output_prefix_dict.get(trait)}_intervals",
                logger,
                file_format=output_format,
                index=False,
            )
            tasks.extend([seg_task, int_task])
    elif function_name.__name__ == "_meta_analysis":
        df_metaanalysis = delayed(_run_extraction)(
            tiledb_uri,
            tiledb_cfg,
            trait_id_list,
            None,
            **kwargs,
        )
        result = delayed(write_table)(
            df_metaanalysis, f"{output_prefix}_meta_analysis", logger, file_format=output_format, index=False
        )
        tasks.append(result)
    else:
        for trait in trait_id_list:
            extracted_df = delayed(_run_extraction)(
                tiledb_uri,
                tiledb_cfg,
                trait,
                output_prefix_dict.get(trait),
                **kwargs,
            )
            transformed_df = delayed(_run_transformation)(extracted_df, group, trait)
            result = delayed(write_table)(
                transformed_df, output_prefix_dict.get(trait), logger, file_format=output_format, index=False
            )
            tasks.append(result)

    # Handle single-batch case if batch_size <= 0 or len(tasks) <= batch_size
    if batch_size <= 0 or len(tasks) <= batch_size:
        logger.info(f"Running all tasks in a single batch ({len(tasks)} items)")
        compute(*tasks, scheduler=dask_client)
        logger.info("Single batch completed.", flush=True)
    else:
        total_batches = math.ceil(len(tasks) / batch_size)
        for i in range(0, len(tasks), batch_size):
            batch_no = i // batch_size + 1
            batch = tasks[i : i + batch_size]
            logger.info(f"Running batch {batch_no}/{total_batches} ({min(batch_size, len(tasks) - i)} items)")
            compute(*batch, scheduler=dask_client)
            logger.info(f"Batch {batch_no} completed.", flush=True)


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
        default="BETA,SE,EAF,MLOG10P,EA,NEA",
        help="string delimited by comma with the attributes to export",
    ),
)
@cloup.option_group(
    "Meta-analysis options",
    cloup.option("--meta-analysis", default=False, is_flag=True, help="Option to run meta-analysis"),
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
    cloup.option(
        "--maf",
        default=0.01,
        help="MAF filter to apply before locusbreaker",
    ),
    cloup.option(
        "--phenovar",
        default=False,
        is_flag=True,
        help="Boolean to compute phenovariance (Work in progress, not fully implemented yet)",
    ),
    cloup.option(
        "--locus-flanks",
        default=100000,
        help="Flanking regions (in bp) to extend each locus in both directions (default: 100000)",
    ),
)
@cloup.option_group(
    "Regions or SNP ID filtering options",
    cloup.option(
        "--get-regions-snps",
        default=None,
        help="Bed (or CHR,POS) file with regions or SNP list to filter",
    ),
    cloup.option(
        "--skip-meta",
        default=False,
        is_flag=True,
        help="Do not add metadata columns (default: False)",
    ),
    cloup.option(
        "--nest",
        default=False,
        is_flag=True,
        help="Estimate effective population size (Work in progress, not fully implemented yet)",
    ),
)
@cloup.option_group(
    "P-value filtering options",
    cloup.option(
        "--pvalue-thr",
        default=0,
        help="Minimum -log10(p-value) threshold to filter significant SNPs",
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
    "Option to query metadata before export",
    cloup.option(
        "--case-sensitive",
        default=False,
        is_flag=True,
        help="Perform case-sensitive matching on query values (default: False).",
    ),
    cloup.option(
        "--exact-match",
        default=False,
        is_flag=True,
        help="Perform exact match on query values (default: False).",
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
    pvalue_thr: float,
    hole_size: int,
    phenovar: bool,
    nest: bool,
    maf: float,
    locus_flanks: int,
    locusbreaker: bool,
    meta_analysis: bool,
    get_regions_snps: str | None,
    skip_meta: bool,
    plot_out: bool,
    color_thr: str,
    s_value: int,
    case_sensitive: bool,
    exact_match: bool,
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
        objs = query_mongo_obj(search_topics, obj, case_sensitive=case_sensitive, exact_match=exact_match)

    meta_df = dataframe_from_mongo_objs(output_fields, objs)

    # Write metadata query result
    path = Path(output_prefix)
    output_path = path.with_suffix("").with_name(path.stem + "_meta")
    kwargs = {"index": False}
    log_msg = f"{len(objs)} results found. Writing to {output_path}.csv"
    write_table(meta_df, str(output_path), logger, file_format="csv", log_msg=log_msg, **kwargs)

    # Create an output prefix dictionary to generate output filenames
    source_id_column = MetadataEnum.get_source_id_field()
    output_prefix_dict = create_output_prefix_dict(meta_df, output_prefix, source_id_column=source_id_column)

    # Process according to selected options
    if get_dask_deployment(ctx) not in dask_deployment_types:
        logger.error(f"A valid dask deployment type must be set from: {dask_deployment_types}")
        raise SystemExit(1)

    with manage_daskcluster(ctx) as client:
        batch_size = get_dask_batch_size(ctx)
        grouped = meta_df.groupby(MetadataEnum.get_tiledb_grouping_fields(), observed=False)
        for name, group in grouped:
            group_name = "_".join(name)
            logger.info(f"Processing the group {group_name}")
            tiledb_uri = join_path(uri, group_name)
            logger.debug(f"tiledb_uri: {tiledb_uri}")

            # Build a per‑group output‑prefix dict
            _output_prefix_dict = {
                key: f"{output_prefix}_{group_name}_{value[len(output_prefix) + 1 :]}"
                for key, value in output_prefix_dict.items()
            }

            _meta_df = group if not skip_meta else group["data_id"]

            # Common argument list
            common_args = [
                tiledb_uri,  # <-- URI, not an opened array
                cfg,
                _meta_df,
                attr,
                batch_size,
                _output_prefix_dict,
                output_format,
            ]

            # Dispatch the appropriate extraction routine
            match (locusbreaker, get_regions_snps, meta_analysis):
                case (True, _, _):
                    _process_function_tasks(
                        *common_args,
                        function_name=_process_locusbreaker,
                        maf=maf,
                        hole_size=hole_size,
                        pvalue_sig=pvalue_sig,
                        pvalue_limit=pvalue_limit,
                        phenovar=phenovar,
                        locus_flanks=locus_flanks,
                        dask_client=client,
                    )
                case (_, str() as bed_fp, _):
                    _process_function_tasks(
                        *common_args,
                        function_name=extract_regions_snps,
                        regions_snps=bed_fp,
                        plot_out=plot_out,
                        color_thr=color_thr,
                        s_value=s_value,
                        dask_client=client,
                    )
                case (_, _, True):
                    _process_function_tasks(
                        *common_args,
                        function_name=_meta_analysis,
                        output_prefix=output_prefix,
                        dask_client=client,
                    )
                case _:
                    _process_function_tasks(
                        *common_args,
                        function_name=extract_full_stats,
                        pvalue_thr=pvalue_thr,
                        plot_out=plot_out,
                        color_thr=color_thr,
                        s_value=s_value,
                        dask_client=client,
                    )
