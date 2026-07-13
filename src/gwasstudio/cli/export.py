import math
from collections.abc import Callable
from pathlib import Path

import click
import cloup
import pandas as pd
import tiledb
from dask import compute, delayed
from dask.distributed import Client

from gwasstudio import logger
from gwasstudio.cli.path_utils import compose_tiledb_uri, join_path
from gwasstudio.cli.region_io import read_to_bed, read_trait_snps
from gwasstudio.cli.utils import (
    create_config_from_context,
    load_yaml_file,
    write_if_not_empty,
    write_table,
)
from gwasstudio.core import ConfigurationError, InvalidInputError
from gwasstudio.core.config import get_dask_batch_size, get_dask_deployment, get_tiledb_config
from gwasstudio.core.enums import MetadataEnum
from gwasstudio.core.query import query_metadata as core_query_metadata
from gwasstudio.dask_client import dask_deployment_types, manage_daskcluster
from gwasstudio.methods.extraction_methods import extract_full_stats, extract_regions_leadsnps, extract_regions_snps
from gwasstudio.methods.locus_breaker import _process_locusbreaker
from gwasstudio.methods.meta_analysis import _meta_analysis


def create_output_prefix_dict(df: pd.DataFrame, output_prefix: str, source_id_column: str) -> dict:
    """
    Generates a dictionary mapping data IDs to output prefixes based on column values.
    If multiple link_id values exist for a data ID, they are joined with '_'.

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

    # Determine the column(s) to use for prefixing
    has_link_id = "link_id" in df.columns
    has_source_id = source_id_column in df.columns

    if has_link_id and has_source_id:
        # Aggregate multiple link IDs per data ID, e.g. one seq ID map to multiple UniProts
        grouped = df.groupby(key_column, as_index=False).agg(
            link_id=("link_id", lambda x: "_".join(sorted(set(map(str, x))))), source_id=(source_id_column, "first")
        )
        grouped[value_column] = output_prefix + "_" + grouped["link_id"] + "_" + grouped["source_id"].astype(str)

        # Create dictionary mapping data IDs and link IDs to prefixes
        output_prefix_dict = dict(zip(grouped[key_column], grouped[value_column]))
        logger.debug("Output prefix dictionary created with link_id")
    else:
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
    trait_snps: str | None = None,
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
        traits: str | list[str],
        out_prefix: str | None,
        **inner_kwargs,
    ) -> pd.DataFrame:
        """Open the TileDB array on the worker and invoke ``function_name``."""
        # Open a *read‑only* handle on the worker.
        with tiledb.open(uri, mode="r", config=cfg) as arr:
            # ``function_name`` expects the opened array as its first argument.
            return function_name(arr, traits, out_prefix, **inner_kwargs)

    def _run_transformation(
        gwas_df: pd.DataFrame, meta_df: pd.DataFrame, trait_id: str, link_ids: list | None = None
    ) -> pd.DataFrame:
        #  Optional metadata broadcast – only used when ``skip_meta`` is False.
        if isinstance(group, pd.Series):
            return gwas_df

        id_col = "data_id"
        meta_row = meta_df.loc[meta_df[id_col] == trait_id].iloc[0]
        # meta_dict = meta_row.squeeze().to_dict()
        meta_dict = {
            f"meta_{k}": v for k, v in meta_row.drop(["data_id", "output_prefix"], errors="ignore").to_dict().items()
        }
        if trait_snps:
            meta_dict["meta_link_id"] = "_".join(sorted(map(str, link_ids)))

        broadcast = {col: [val] * len(gwas_df) for col, val in meta_dict.items()}
        return gwas_df.assign(**broadcast)

    # Prepare kwargs for the downstream extraction routine.
    kwargs["attributes"] = attr.split(",") if attr else None

    if regions_snps:
        kwargs["regions_snps"] = delayed(read_to_bed)(regions_snps)
    if trait_snps:
        all_trait_snps = delayed(read_trait_snps)(trait_snps)

    trait_id_list = group["data_id"].unique().tolist() if not isinstance(group, pd.Series) else group.unique().tolist()
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
    elif function_name.__name__ == "extract_regions_snps":
        for trait in trait_id_list:
            extracted_tuple = delayed(_run_extraction)(
                tiledb_uri,
                tiledb_cfg,
                trait,
                output_prefix_dict.get(trait),
                **kwargs,
            )
            extracted_df = delayed(lambda t: t[0])(extracted_tuple)
            pvalue_filt_df = delayed(lambda t: t[1])(extracted_tuple)
            transformed_df = delayed(_run_transformation)(extracted_df, group, trait, None)
            result = delayed(write_if_not_empty)(
                transformed_df,
                output_prefix_dict.get(trait),
                logger,
                file_format=output_format,
                index=False,
            )
            result_pvalue_filt = delayed(write_if_not_empty)(
                pvalue_filt_df,
                f"{output_prefix_dict.get(trait)}_pvalue_filt",
                logger,
                file_format=output_format,
                index=False,
            )
            tasks.extend([result, result_pvalue_filt])
    else:
        for trait in trait_id_list:
            if trait_snps and "link_id" in group.columns:
                link_ids = group.loc[group["data_id"] == trait, "link_id"].unique()
                if len(link_ids) and link_ids[0] is not None:
                    kwargs["trait_snps"] = all_trait_snps[all_trait_snps["SOURCE_ID"].isin(link_ids)]
            elif trait_snps:
                kwargs["trait_snps"] = pd.DataFrame()
            extracted_df = delayed(_run_extraction)(
                tiledb_uri,
                tiledb_cfg,
                trait,
                output_prefix_dict.get(trait),
                **kwargs,
            )
            transformed_df = delayed(_run_transformation)(extracted_df, group, trait, link_ids if trait_snps else None)
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
        help="BED (CHR\tSTART\tEND) or SNP list (CHR,POS) file paths, or string equivalents: (CHR,START,END;CHR,START,END) or (CHR,POS;CHR,POS)",
    ),
    cloup.option(
        "--pvalue-filt",
        default=0.0,
        help="Minimum -log10(p-value) threshold to keep significant filtered SNPs",
    ),
    cloup.option(
        "--skip-out",
        default=False,
        is_flag=True,
        help="Do not write regions output (default: False)",
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
    "Trait-specific lead-SNP search options",
    cloup.option(
        "--get-regions-leadsnps",
        default=None,
        help="A DataFrame containing SOURCE_ID (trait), CHR, POS, EA and NEA for lead-SNP search",
    ),
    cloup.option(
        "--cis-flanks",
        default=500000,
        help="Flanking region (in bp) around POS for the search of CIS lead-SNP (default: 500000)",
    ),
    cloup.option(
        "--trans-flanks",
        default=1000000,
        help="Flanking region (in bp) around POS for the search of TRANS lead-SNP (default: 1000000)",
    ),
    cloup.option(
        "--exact-alleles",
        default=False,
        is_flag=True,
        help="Whether exact lead match includes also EA and NEA, or only CHR and POS (default: False)",
    ),
)
@cloup.option_group(
    "P-value filtering options",
    cloup.option(
        "--pvalue-thr",
        default=0.0,
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
    pvalue_filt: float,
    get_regions_leadsnps: str | None,
    cis_flanks: int,
    trans_flanks: int,
    exact_alleles: bool,
    skip_meta: bool,
    skip_out: bool,
    plot_out: bool,
    color_thr: str,
    s_value: int,
    case_sensitive: bool,
    exact_match: bool,
) -> None:
    """Export summary statistics based on selected options."""

    # Validate search file exists
    if not Path(search_file).exists():
        raise InvalidInputError(f"Search file not found: {search_file}")

    # Load YAML file
    yaml_content = load_yaml_file(search_file)

    # Create GWASStudioConfig from context (for core compatibility)
    try:
        config = create_config_from_context(ctx)
    except Exception as e:
        raise ConfigurationError(f"Failed to create configuration from context: {str(e)}")

    # Query metadata using core function with YAML template
    # The core function will handle parsing, validation, and query options
    query_results, output_fields = core_query_metadata(
        yaml_template=yaml_content,
        config=config,
        case_sensitive=case_sensitive,
        exact_match=exact_match,
    )

    if not query_results:
        logger.info("No results found.")
        return

    # Extract search topics from yaml_content for backwards compatibility
    yaml_query_fields = yaml_content.get("query_fields", yaml_content)

    if plot_out:
        if "data_id" not in yaml_query_fields:
            logger.error("Plotting option is enabled but no data_ids is provided in the search file.")
            exit(1)
        # Extract data_ids from query_results
        data_ids = [r.get("data_id") for r in query_results if r.get("data_id")]
        if len(data_ids) > 20:
            logger.error(
                "Plotting option is enabled but too many data_ids are provided in the search file. Please limit to 20 data_ids."
            )
            exit(1)

    # Convert query_results to DataFrame
    # output_fields contains the list of fields requested in the output
    if output_fields:
        meta_df = pd.DataFrame(query_results, columns=output_fields)
    else:
        # If no output fields specified, use all available keys from first result
        if query_results:
            output_fields = list(query_results[0].keys())
            meta_df = pd.DataFrame(query_results, columns=output_fields)
        else:
            meta_df = pd.DataFrame()

    # Add link_id column if trait or notes fields are present in yaml_content
    # This is needed for get_regions_leadsnps functionality
    # Note: trait and notes fields are now stored as dicts (not JSON strings) due to JSONField
    yaml_query_fields = yaml_content.get("query_fields", yaml_content)
    if get_regions_leadsnps and ("trait" in yaml_query_fields or "notes" in yaml_query_fields):
        # Extract the first trait or notes subfield from YAML
        trait_data = yaml_query_fields.get("trait", [])
        notes_data = yaml_query_fields.get("notes", [])

        field_to_use = None
        subfield = None

        if trait_data:
            field_to_use = "trait"
            first_item = trait_data[0] if trait_data else {}
        elif notes_data:
            field_to_use = "notes"
            first_item = notes_data[0] if notes_data else {}

        if field_to_use and isinstance(first_item, dict):
            subfield = next(iter(first_item.keys()))

        if field_to_use and subfield:
            # Map the YAML key to the MongoDB field name (underscore-joined)
            mongodb_field_name = f"{field_to_use}_{subfield}"
            if mongodb_field_name in meta_df.columns:
                # Extract link_id from the dict stored in the MongoDB field
                def extract_link_id(value):
                    if pd.isna(value):
                        return None
                    if isinstance(value, dict):
                        return value.get(subfield)
                    return value

                meta_df["link_id"] = meta_df[mongodb_field_name].apply(extract_link_id)

    # Write metadata query result
    path = Path(output_prefix)
    output_path = path.with_suffix("").with_name(path.stem + "_meta")
    kwargs = {"index": False}
    log_msg = f"{len(query_results)} results found. Writing to {output_path}.csv"
    write_table(meta_df, str(output_path), logger, file_format="csv", log_msg=log_msg, **kwargs)

    # Create an output prefix dictionary to generate output filenames
    source_id_column = MetadataEnum.get_source_id_field()
    output_prefix_dict = create_output_prefix_dict(meta_df, output_prefix, source_id_column=source_id_column)

    # Process according to selected options
    if get_dask_deployment(config) not in dask_deployment_types:
        logger.error(f"A valid dask deployment type must be set from: {dask_deployment_types}")
        raise SystemExit(1)

    cfg = get_tiledb_config(config)

    with manage_daskcluster(config) as client:
        batch_size = get_dask_batch_size(config)
        grouped = meta_df.groupby(MetadataEnum.get_tiledb_grouping_fields(), observed=False)
        for name, group in grouped:
            group_name, tiledb_uri = compose_tiledb_uri(uri, name, logger)
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
            match (locusbreaker, get_regions_snps, get_regions_leadsnps, meta_analysis):
                case (True, _, _, _):
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
                case (_, str() as bed_fp, _, _):
                    _process_function_tasks(
                        *common_args,
                        function_name=extract_regions_snps,
                        regions_snps=bed_fp,
                        pvalue_filt=pvalue_filt,
                        skip_out=skip_out,
                        plot_out=plot_out,
                        color_thr=color_thr,
                        s_value=s_value,
                        dask_client=client,
                    )
                case (_, _, str() as traitsnp_fp, _):
                    _process_function_tasks(
                        *common_args,
                        function_name=extract_regions_leadsnps,
                        trait_snps=traitsnp_fp,
                        cis_flanks=cis_flanks,
                        trans_flanks=trans_flanks,
                        exact_alleles=exact_alleles,
                        dask_client=client,
                    )
                case (_, _, _, True):
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
