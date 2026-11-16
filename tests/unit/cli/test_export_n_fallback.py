"""Regression tests: the ``N`` fallback must work in every ``--meta`` mode.

``N`` is not necessarily a TileDB attribute; when it is absent it is synthesized
from the metadata field ``total_samples`` (see :func:`_add_n_fallback`).

``extract_regions_snps`` returns a *tuple* of DataFrames, so the backend-level
N fallback does not apply to it — the N column is only added by the
``_run_transformation`` step. Before the fix, that step bailed out early when
``--meta`` was ``none``/``sidefile`` (the metadata row had been reduced to a
``data_id`` Series), so ``N`` was silently dropped. These tests pin the behavior
for both ``--meta none`` and the (regression) ``--meta full`` case, plus the
"no total_samples -> no N" case.
"""

import types
from unittest.mock import patch

import pandas as pd

import gwasstudio.cli as _cli
import gwasstudio.cli.export as export_mod
from gwasstudio.cli.export import _process_function_tasks

# Importing the export *module* binds gwasstudio.cli.export to the module, which would
# otherwise shadow the package's lazy `export -> command` resolution (asserted by
# tests/unit/cli/test_lazy_cli.py::test_package_lazy_attribute_access). Reconcile the
# two so `cli.export` keeps resolving to the command as the lazy design intends. This
# test uses the module directly via `export_mod`, so it is unaffected.
_cli.export = export_mod.export

DATA_ID = "seq.6478.2"
TOTAL_SAMPLES = 5000


def _full_group() -> pd.DataFrame:
    """One metadata row for a single dataset, as returned by core_query_metadata."""
    return pd.DataFrame(
        [
            {
                "data_id": DATA_ID,
                "total_samples": TOTAL_SAMPLES,
                "notes_trait": "BMI",
            }
        ]
    )


def _resolved_total_samples(group: pd.DataFrame) -> dict:
    """Mirror the resolution step the main export loop performs from the full row."""
    trait_to_total_samples: dict[str, object] = {}
    if "total_samples" in group.columns:
        for _, row in group.iterrows():
            if pd.notna(row["total_samples"]):
                trait_to_total_samples[row["data_id"]] = row["total_samples"]
    return trait_to_total_samples


def _run(meta_df, trait_to_total_samples: dict):
    """Drive _process_function_tasks through the extract_regions_snps path."""
    # extract_regions_snps returns a TUPLE (data, pvalue_flags) with no N column.
    data_df = pd.DataFrame({"CHR": ["1"], "POS": [100], "BETA": [0.1], "SE": [0.01], "MLOG10P": [5.0]})
    pvalue_df = pd.DataFrame(columns=["CHR", "START", "END", "PVALUE_FILT_FLAG"])
    fake_backend = types.SimpleNamespace(
        extract=lambda uri, cfg, trait, out_prefix, fn, total_samples=None, **kw: (data_df.copy(), pvalue_df.copy())
    )

    written: dict = {}

    def fake_write_if_not_empty(df, where, logger, compression=True, file_format="csv", log_msg="none", **kw):
        written[where] = df
        return df

    with (
        patch.object(export_mod, "read_to_bed", lambda fp, buf=0: pd.DataFrame()),
        patch.object(export_mod, "write_if_not_empty", fake_write_if_not_empty),
    ):
        _process_function_tasks(
            "file://fake-array",
            {},
            meta_df,
            "BETA,SE,EAF,MLOG10P,EA,NEA,N",
            0,
            {DATA_ID: f"out_{DATA_ID}"},
            "csv",
            trait_to_total_samples,
            backend=fake_backend,
            function_name=export_mod.extract_regions_snps,
            regions_snps="ignored.bed",
            region_buffer=100000,
            pvalue_filt=0.0,
            skip_out=False,
            filter_multiallelic=False,
            plot_out=False,
            color_thr="0.05",
            s_value=3,
            dask_client=None,
        )

    # The main data output is the one carrying the GWAS columns (not the pvalue flags).
    main_df = next(df for df in written.values() if "BETA" in df.columns)
    return main_df


def test_meta_none_adds_n_for_regions_snps():
    # --meta none: the metadata row is reduced to a data_id Series.
    full_group = _full_group()
    series_group = full_group["data_id"]
    trait_to_total_samples = _resolved_total_samples(full_group)

    main_df = _run(series_group, trait_to_total_samples)

    assert "N" in main_df.columns, f"N missing from output columns: {list(main_df.columns)}"
    assert (main_df["N"] == TOTAL_SAMPLES).all()
    # No meta_* broadcast columns when --meta none.
    assert not any(c.startswith("meta_") for c in main_df.columns)


def test_meta_full_still_adds_n_and_broadcasts():
    # --meta full: the full metadata row is passed through (regression guard).
    full_group = _full_group()
    trait_to_total_samples = _resolved_total_samples(full_group)

    main_df = _run(full_group, trait_to_total_samples)

    assert "N" in main_df.columns
    assert (main_df["N"] == TOTAL_SAMPLES).all()
    # meta columns ARE broadcast when --meta full.
    assert "meta_notes_trait" in main_df.columns


def test_no_total_samples_produces_no_n():
    # No total_samples in metadata -> no N column (mirrors integration Case 3).
    group = pd.DataFrame([{"data_id": DATA_ID, "total_samples": None, "notes_trait": "BMI"}])
    trait_to_total_samples = _resolved_total_samples(group)
    assert trait_to_total_samples == {}

    main_df = _run(group["data_id"], trait_to_total_samples)

    assert "N" not in main_df.columns, f"N unexpectedly present: {list(main_df.columns)}"
