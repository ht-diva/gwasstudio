"""
GWASStudio TileDB Storage Backend
==================================

:class:`TileDBStorageBackend` implements :class:`gwasstudio.core.storage.base.StorageBackend`
by *delegating* to the TileDB code paths the CLI already uses — so behaviour is
identical to the existing ``ingest``/``export`` commands:

- **Ingest**: schema creation via :class:`gwasstudio.utils.tdb_schema.TileDBSchemaCreator`
  (with the same per-scheme config selection as ``ingest_to_s3``/``ingest_to_fs``),
  then the exact append primitive of :func:`gwasstudio.cli.utils.process_and_ingest`
  (``tiledb.from_pandas(..., index_dims=["CHR", "TRAITID", "POS"], mode="append")``).
  When ``file_path`` is supplied and no DataFrame is, the file-based pipeline
  (validated read + dtype conversion + hashing) is delegated to
  :func:`gwasstudio.cli.utils.process_and_ingest` verbatim.
- **Query**: the array is opened read-only exactly like
  :mod:`gwasstudio.cli.export` does — ``tiledb.open(uri, mode="r", config=cfg)``
  with the merged ``vfs``+``sm`` config — and handed to
  :func:`gwasstudio.methods.extraction_methods.extract_full_stats` (full trait)
  or :func:`gwasstudio.methods.extraction_methods.extract_regions_snps`
  (region/SNP filtering), preserving the MLOG10P-attribute fallback that lives
  in :func:`gwasstudio.methods.extraction_methods.tiledb_array_query`.
- **Existence**: :func:`gwasstudio.cli.s3.does_uri_path_exist` for ``s3://``
  URIs, ``Path.exists`` otherwise — the same checks ``ingest`` performs.

Design notes
------------
- Imports of the CLI layer (``gwasstudio.cli.*``) and of ``boto3`` are deferred
  into the methods that use them, so importing this module (and
  :func:`gwasstudio.core.storage.get_storage_backend`) does not pull the
  Dask/CLI stack into light commands — the lazy-import invariant from
  ``tests/unit/cli/test_lazy_cli.py`` stays intact.
- The backend never alters the wrapped calls: it only resolves the array URI,
  picks the same config the CLI would pick, and forwards the same arguments.
- The "project" of a TileDB array is identified by its URI
  (``warehouse_uri`` + ``(project, study)`` grouping, see
  :func:`gwasstudio.cli.path_utils.compose_tiledb_uri`); the ``project_id``
  argument of the ABC methods is the trait/data identifier *inside* the array
  (the ``TRAITID`` dimension value, i.e. the metadata ``data_id``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import tiledb

from gwasstudio import logger
from gwasstudio.core.config import GWASStudioConfig, get_tiledb_config
from gwasstudio.core.hashing import Hashing
from gwasstudio.core.storage.base import StorageBackend, StorageError

# Sparse-array index dimensions — identical to
# ``gwasstudio.cli.utils.process_and_ingest`` and
# ``gwasstudio.methods.extraction_methods.TILEDB_DIMS``.
INDEX_DIMS: tuple[str, ...] = ("CHR", "TRAITID", "POS")

# CLI default for ``export --attr`` (required option, this is its default).
DEFAULT_ATTRIBUTES: tuple[str, ...] = ("BETA", "SE", "EAF", "MLOG10P", "EA", "NEA")


class TileDBError(StorageError):
    """Exception raised for TileDB-specific errors (in the base StorageError hierarchy)."""

    pass


def _build_regions_df(region: dict[str, Any] | list[dict[str, Any]] | None, snp_list: list[Any] | None) -> pd.DataFrame:
    """Build the ``CHR/START/END`` frame consumed by ``extract_regions_snps``.

    Args:
        region: A single region dict or a list of them. Keys are case-insensitive:
            ``{"CHR": 1, "START": 1000, "END": 2000}`` (also accepts ``chr/start/end``).
        snp_list: List of SNPs, each given as ``(chr, pos)`` / ``[chr, pos]``,
            ``{"CHR": 1, "POS": 123}``, or a string ``"CHR:POS[:EA:NEA]"``.
            Each SNP becomes a one-base interval (``END == START + 1``), which is
            exactly the condition ``extract_regions_snps`` uses to switch to its
            SNP-indexing mode.

    Returns:
        pd.DataFrame: Frame with columns ``["CHR", "START", "END"]`` (empty if no input).
    """
    rows: list[dict[str, Any]] = []
    if region is not None:
        regions = region if isinstance(region, (list, tuple)) else [region]
        for item in regions:
            norm = {str(k).upper(): v for k, v in dict(item).items()}
            rows.append({"CHR": _norm_chr(norm["CHR"]), "START": int(norm["START"]), "END": int(norm["END"])})
    if snp_list:
        for snp in snp_list:
            if isinstance(snp, dict):
                norm = {str(k).upper(): v for k, v in snp.items()}
                chr_, pos = norm["CHR"], int(norm["POS"])
            elif isinstance(snp, (list, tuple)):
                chr_, pos = snp[0], int(snp[1])
            else:
                parts = str(snp).split(":")
                chr_, pos = parts[0], int(parts[1])
            rows.append({"CHR": _norm_chr(chr_), "START": pos, "END": pos + 1})
    if not rows:
        return pd.DataFrame(columns=["CHR", "START", "END"])
    return pd.DataFrame(rows, columns=["CHR", "START", "END"])


def _norm_chr(value: Any) -> int | str:
    """Normalise a chromosome value: numeric when possible (the CHR dim is uint8)."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


class TileDBStorageBackend(StorageBackend):
    """
    TileDB implementation of :class:`StorageBackend`.

    The backend is bound to one *warehouse*: the base URI under which the
    per-project TileDB arrays live (e.g. ``s3://bucket/dataset`` or a local
    path). The concrete array URI used for a call is resolved in this order:

    1. an explicit ``uri=...`` keyword argument of the call (full array URI);
    2. the constructor's ``warehouse_uri`` joined with ``(project, study)``
       via :func:`gwasstudio.cli.path_utils.compose_tiledb_uri` (the same rule
       the CLI uses); a bare ``warehouse_uri`` is used as-is;
    3. otherwise a :class:`TileDBError` is raised.

    No connection is opened in the constructor: arrays are opened per call,
    read-only and *in the calling process* — the same pattern the export
    command uses inside its Dask workers.
    """

    def __init__(
        self,
        config: GWASStudioConfig,
        warehouse_uri: str | None = None,
        project: str | None = None,
        study: str | None = None,
    ) -> None:
        """
        Initialize the TileDB storage backend.

        Args:
            config: GWASStudio configuration (supplies the TileDB ``vfs``/``sm`` config).
            warehouse_uri: Base warehouse URI for per-project arrays (optional).
            project: Project part of the array URI grouping (optional).
            study: Study part of the array URI grouping (optional).
        """
        self.config = config
        self.warehouse_uri = warehouse_uri
        self.project = project
        self.study = study

    # ── URI / config resolution ───────────────────────────────────────────

    def _resolve_uri(self, uri: str | None = None) -> str:
        """Resolve the TileDB array URI for a call (see class docstring for precedence)."""
        if uri:
            return uri
        if self.warehouse_uri:
            parts = tuple(part for part in (self.project, self.study) if part)
            if parts:
                from gwasstudio.cli.path_utils import compose_tiledb_uri

                _, array_uri = compose_tiledb_uri(self.warehouse_uri, parts, logger)
                return array_uri
            return self.warehouse_uri
        raise TileDBError(
            "Cannot resolve a TileDB array URI: pass uri=... to the call, or construct the "
            "backend with warehouse_uri (and optionally project/study)."
        )

    @staticmethod
    def _scheme(uri: str) -> str:
        """URI scheme via the shared ``parse_uri`` helper ('' for local paths)."""
        from gwasstudio.cli.utils import parse_uri

        scheme, _, _ = parse_uri(uri)
        return scheme

    def _write_cfg(self, uri: str) -> dict[str, Any]:
        """TileDB write config for ``uri``, mirroring ``ingest_to_s3``/``ingest_to_fs``.

        S3 warehouses use the merged ``vfs``+``sm`` config; local filesystem
        warehouses use the ``sm``-only config (as ``ingest_to_fs`` does).
        """
        if self._scheme(uri) == "s3":
            return get_tiledb_config(self.config)
        return get_tiledb_config(self.config, prefix="sm")

    def _ensure_schema(self, uri: str, additional_columns: list[str]) -> None:
        """Create the schema if missing — the exact checks ``ingest`` performs.

        S3: skip when ``does_uri_path_exist`` is truthy (create with the merged config).
        Local: skip when the path exists (create with an empty config, as ``ingest_to_fs`` does).

        Tolerates the concurrent-creation race: if a sibling Dask worker created
        the schema between our existence check and our ``create`` call, we log
        a debug message and proceed (idempotent no-op).
        """
        from gwasstudio.utils.tdb_schema import TileDBSchemaCreator

        try:
            if self._scheme(uri) == "s3":
                from gwasstudio.cli.s3 import does_uri_path_exist

                cfg = get_tiledb_config(self.config)
                if not does_uri_path_exist(uri, cfg):
                    logger.info("Creating TileDB schema")
                    TileDBSchemaCreator(uri, cfg, additional_attributes=list(additional_columns)).create_schema()
            else:
                from gwasstudio.cli.utils import parse_uri

                _, _, path = parse_uri(uri)
                if not Path(path).exists():
                    logger.info("Creating TileDB schema")
                    TileDBSchemaCreator(uri, {}, additional_attributes=list(additional_columns)).create_schema()
        except (RuntimeError, tiledb.TileDBError) as e:
            msg = str(e).lower()
            if "already exists" in msg or "file exists" in msg or "cannot create" in msg:
                logger.debug(f"TileDB schema already exists (concurrent creation): {uri}")
            else:
                raise

    # ── StorageBackend implementation ─────────────────────────────────────

    def extract(
        self,
        uri: str,
        cfg: dict[str, str],
        trait: str | list[str],
        output_prefix: str | None,
        function: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Open the TileDB array read-only and invoke the given extraction function.

        This is the primitive the CLI export pipeline uses inside Dask workers:
        the array is opened *locally* (never serialized across processes) and
        the function receives the open array as its first argument.

        Args:
            uri: Full TileDB array URI (e.g. ``s3://bucket/dataset`` or a local path).
            cfg: TileDB config dict (merged vfs+sm or sm-only, as the CLI selects).
            trait: Trait identifier (``TRAITID`` dimension value) or list (meta-analysis).
            output_prefix: Output prefix for plots (``None`` to skip).
            function: The extraction callable — any of ``extract_full_stats``,
                ``extract_regions_snps``, ``extract_regions_leadsnps``,
                ``_process_locusbreaker``, ``_meta_analysis``.
            **kwargs: Forwarded to the extraction function.

        Returns:
            Whatever the extraction function returns (``pd.DataFrame`` or tuple thereof).

        Raises:
            TileDBError: If the array cannot be opened or the function fails.
        """
        try:
            with tiledb.open(uri, mode="r", config=cfg) as arr:
                return function(arr, trait, output_prefix, **kwargs)
        except Exception as e:
            raise TileDBError(f"Extraction failed for trait {trait!r} at {uri}: {str(e)}") from e

    def ingest_data(
        self,
        df: pd.DataFrame | None = None,
        project_name: str = "",
        **kwargs: Any,
    ) -> str:
        """
        Ingest a DataFrame (or a file) into the project's TileDB array.

        Args:
            df: DataFrame with the ``CHR/POS/EA/NEA/EAF/SE/BETA`` columns. When
                ``None``, ``file_path`` must be given and the file-based CLI
                pipeline (validated read + dtypes + append) is used verbatim.
            project_name: Trait identifier written into the ``TRAITID`` dimension
                (used when ``df`` has no ``TRAITID`` column and no ``file_path``
                is given).
            **kwargs:
                - ``uri`` (str): explicit full array URI (overrides the constructor's).
                - ``file_path`` (str | Path): source file; its hash
                  (``Hashing().compute_hash``) is used as ``TRAITID``, exactly like
                  ``process_and_ingest``.
                - ``additional_columns`` / ``add_cols`` (list[str]): extra
                  attributes (``MLOG10P``/``N``) for schema creation.

        Returns:
            str: The ``TRAITID`` (data id) under which the data was ingested.

        Raises:
            StorageError: If ingestion fails.
        """
        uri = self._resolve_uri(kwargs.get("uri"))
        file_path = kwargs.get("file_path")
        additional_columns = list(kwargs.get("additional_columns") or kwargs.get("add_cols") or [])

        self._ensure_schema(uri, additional_columns)

        if df is None:
            if not file_path:
                raise TileDBError("ingest_data requires a DataFrame or a 'file_path' keyword argument")
            # 1:1 delegation to the CLI pipeline (read + validate + hash + append).
            from gwasstudio.cli.utils import process_and_ingest

            cfg = self._write_cfg(uri)
            process_and_ingest(str(file_path), uri, cfg, additional_columns)
            return Hashing().compute_hash(str(file_path))

        # DataFrame path: same TRAITID semantics and the same append primitive
        # as ``gwasstudio.cli.utils.process_and_ingest`` (tiledb.from_pandas append).
        if "TRAITID" not in df.columns:
            if file_path:
                trait_id = Hashing().compute_hash(str(file_path))
            elif project_name:
                trait_id = project_name
            else:
                raise TileDBError(
                    "DataFrame has no 'TRAITID' column: provide project_name or a file_path "
                    "keyword argument to derive one."
                )
            df = df.copy()
            df["TRAITID"] = trait_id
        else:
            trait_id = str(df["TRAITID"].iloc[0])

        cfg = self._write_cfg(uri)
        try:
            ctx = tiledb.Ctx(tiledb.Config(cfg))
            tiledb.from_pandas(uri=uri, dataframe=df, index_dims=list(INDEX_DIMS), mode="append", ctx=ctx)
        except Exception as e:
            raise StorageError(f"Failed to write file {file_path} to TileDB at {uri}: {str(e)}") from e
        return trait_id

    def query_data(
        self,
        project_id: str,
        region: dict[str, Any] | list[dict[str, Any]] | None = None,
        snp_list: list[Any] | None = None,
        pval_threshold: float | None = None,
        limit: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Query data from the project's TileDB array.

        ``project_id`` is the trait identifier (``TRAITID`` dimension value, i.e.
        the metadata ``data_id``). Delegates to ``extract_full_stats`` (no
        region/SNP filter) or ``extract_regions_snps`` (region/SNP filter), with
        plotting disabled; ``pval_threshold`` is a -log10(p) threshold applied as
        ``MLOG10P > threshold`` (``0``/``None`` = no filter), matching the CLI's
        ``--pvalue-thr``/``--pvalue-filt`` semantics.

        Args:
            project_id: Trait/data identifier stored in the ``TRAITID`` dimension.
            region: Region dict(s) ``{"CHR": .., "START": .., "END": ..}`` (keys case-insensitive).
            snp_list: List of SNPs as ``(chr, pos)``, ``{"CHR": .., "POS": ..}`` or ``"CHR:POS[:..]"``.
            pval_threshold: Keep only variants with ``MLOG10P`` > threshold (0 = off).
            limit: Maximum number of rows to return (applied after the query; the
                CLI pipeline has no equivalent, this is an additive convenience).
            **kwargs:
                - ``uri`` (str): explicit full array URI (overrides the constructor's).
                - ``attributes`` (list[str]): attributes to fetch (default: the
                  CLI's ``--attr`` default list).
                - ``filter_multiallelic`` (bool): delegate to
                  ``keep_best_multiallelic_variant`` (default False).
                - ``output_prefix`` (str): only used when plotting is enabled.
                - ``plot_out`` (bool), ``color_thr`` (str), ``s_value`` (int):
                  Manhattan-plot options (plotting is off by default).

        Returns:
            pd.DataFrame: The processed results (``SNPID`` front, ``TRAITID`` dropped).

        Raises:
            StorageError: If the query fails.
        """
        from gwasstudio.methods.extraction_methods import extract_full_stats, extract_regions_snps

        uri = self._resolve_uri(kwargs.get("uri"))
        cfg = get_tiledb_config(self.config)
        pthr = float(pval_threshold or 0.0)
        attributes = kwargs.get("attributes") or list(DEFAULT_ATTRIBUTES)
        plot_out = bool(kwargs.get("plot_out", False))

        # Same read-only open the export command uses inside its workers.
        with tiledb.open(uri, mode="r", config=cfg) as arr:
            if region is not None or snp_list is not None:
                regions_snps = _build_regions_df(region, snp_list)
                if regions_snps.empty:
                    return pd.DataFrame()
                df, _ = extract_regions_snps(
                    arr,
                    project_id,
                    output_prefix=kwargs.get("output_prefix", "out"),
                    skip_out=False,
                    filter_multiallelic=bool(kwargs.get("filter_multiallelic", False)),
                    plot_out=plot_out,
                    color_thr=kwargs.get("color_thr", "red"),
                    s_value=int(kwargs.get("s_value", 5)),
                    regions_snps=regions_snps,
                    pvalue_filt=pthr,
                    attributes=attributes,
                )
            else:
                df = extract_full_stats(
                    arr,
                    project_id,
                    output_prefix=kwargs.get("output_prefix", "out"),
                    filter_multiallelic=bool(kwargs.get("filter_multiallelic", False)),
                    plot_out=plot_out,
                    color_thr=kwargs.get("color_thr", "red"),
                    s_value=int(kwargs.get("s_value", 5)),
                    pvalue_thr=pthr,
                    attributes=attributes,
                )

        if limit is not None and not df.empty:
            df = df.head(int(limit))
        return df

    def query_data_stream(
        self,
        project_id: str,
        region: dict[str, Any] | list[dict[str, Any]] | None = None,
        snp_list: list[Any] | None = None,
        pval_threshold: float | None = None,
        chunk_size: int = 10000,
        **kwargs: Any,
    ) -> Any:
        """
        Query data from the storage backend in chunks (streaming).

        Runs the same delegated query as :meth:`query_data` (without ``limit``)
        and yields the result in row-chunks of ``chunk_size``. No rows are
        yielded when the query result is empty.

        Args:
            project_id: Trait/data identifier (see :meth:`query_data`).
            region: Region dict(s) (see :meth:`query_data`).
            snp_list: List of SNPs (see :meth:`query_data`).
            pval_threshold: -log10(p) threshold (see :meth:`query_data`).
            chunk_size: Number of rows per yielded chunk (default 10000).
            **kwargs: Forwarded to :meth:`query_data` (``uri``, ``attributes``, …).

        Yields:
            pd.DataFrame: Chunks of the query result.

        Raises:
            StorageError: If the query fails.
        """
        if chunk_size <= 0:
            raise TileDBError(f"chunk_size must be positive, got {chunk_size}")
        df = self.query_data(
            project_id,
            region=region,
            snp_list=snp_list,
            pval_threshold=pval_threshold,
            **kwargs,
        )
        if df.empty:
            return
        for start in range(0, len(df), chunk_size):
            yield df.iloc[start : start + chunk_size]

    def project_exists(self, project_id: str, **kwargs: Any) -> bool:
        """
        Check whether the project's TileDB array exists.

        ``project_id`` is not used to build the URI (the constructor's
        ``warehouse_uri``/``project``/``study`` or a ``uri=...`` keyword
        argument do); it identifies the array the same way the CLI does — by
        location. For ``s3://`` URIs this is exactly the
        ``does_uri_path_exist`` check ``ingest`` performs; for local paths it is
        ``Path.exists``.

        Args:
            project_id: Trait/data identifier (accepted for interface parity).
            **kwargs: ``uri`` (str) to override the resolved array URI.

        Returns:
            bool: True if the array/warehouse path exists, False otherwise.
        """
        uri = self._resolve_uri(kwargs.get("uri"))
        if self._scheme(uri) == "s3":
            from gwasstudio.cli.s3 import does_uri_path_exist

            return bool(does_uri_path_exist(uri, get_tiledb_config(self.config)))
        from gwasstudio.cli.utils import parse_uri

        _, _, path = parse_uri(uri)
        return Path(path).exists()

    def delete_project(self, project_id: str, **kwargs: Any) -> None:
        """
        Delete the project's TileDB array (all of its files; S3 included via VFS).

        This has no CLI equivalent today; it removes the array's directory
        through ``tiledb.VFS`` (the primitive available in tiledb-py 0.33.x),
        which handles both local paths and ``s3://`` URIs.

        Args:
            project_id: Trait/data identifier (accepted for interface parity).
            **kwargs: ``uri`` (str) to override the resolved array URI.

        Raises:
            TileDBError: If the array is missing or deletion fails.
        """
        uri = self._resolve_uri(kwargs.get("uri"))
        cfg = get_tiledb_config(self.config)
        try:
            vfs = tiledb.VFS(cfg)
            if not vfs.is_dir(uri):
                raise TileDBError(f"TileDB array not found at {uri}")
            vfs.remove_dir(uri)
        except TileDBError:
            raise
        except Exception as e:
            raise TileDBError(f"Failed to delete TileDB array at {uri}: {str(e)}") from e
