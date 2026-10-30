"""Lazy CLI loading tests.

Lightweight commands (``list``, ``meta-query``, ``info``) must not pull in
the heavy Dask/TileDB/boto3 stack that ``export`` and ``ingest`` need.

Import-footprint assertions run in a fresh Python subprocess so they are not
affected by imports that other test modules made in the pytest process.
"""

import subprocess
import sys

import click

from gwasstudio.main import _LAZY_COMMANDS, _lazy_group, _register_lazy_commands, cli_init

_HEAVY = ("dask", "tiledb", "boto3", "botocore", "hvac")


def _run_fresh_interpreter(code: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=300,
    )


def _assert_no_heavy(modules: str) -> None:
    code = (
        f"import {modules}\n"
        "import sys\n"
        f"heavy = [m for m in sys.modules if m.split('.')[0] in {_HEAVY!r}]\n"
        "assert not heavy, heavy\n"
    )
    p = _run_fresh_interpreter(code)
    assert p.returncode == 0, p.stderr


def test_info_command_avoids_heavy_stack():
    _assert_no_heavy("gwasstudio.cli.info")


def test_list_command_avoids_heavy_stack():
    _assert_no_heavy("gwasstudio.cli.list")


def test_meta_query_command_avoids_heavy_stack():
    _assert_no_heavy("gwasstudio.cli.metadata.query")


def test_cli_package_import_is_light():
    code = (
        "import gwasstudio.cli\n"
        "import sys\n"
        f"heavy = [m for m in sys.modules if m.split('.')[0] in {_HEAVY!r}]\n"
        "assert not heavy, heavy\n"
        "assert 'gwasstudio.cli.export' not in sys.modules\n"
        "assert 'gwasstudio.cli.ingest' not in sys.modules\n"
    )
    p = _run_fresh_interpreter(code)
    assert p.returncode == 0, p.stderr


def test_info_runs_end_to_end_without_heavy_stack():
    code = (
        "import sys\n"
        "sys.argv = ['gwasstudio', 'info']\n"
        "from gwasstudio.main import main\n"
        "main()\n"
        f"heavy = [m for m in sys.modules if m.split('.')[0] in {_HEAVY!r}]\n"
        "assert not heavy, heavy\n"
    )
    p = _run_fresh_interpreter(code)
    assert p.returncode == 0, p.stderr


def _fresh_group():
    group = _lazy_group(name="t", help="t")(lambda: None)
    _register_lazy_commands(group)
    return group


def test_all_commands_registered_as_placeholders():
    assert set(cli_init.commands) == set(_LAZY_COMMANDS)
    for name, cmd in cli_init.commands.items():
        assert cmd.name == name
        assert cmd.help  # placeholder carries the help text


def test_get_command_loads_real_command_and_caches():
    group = _fresh_group()
    with click.Context(group) as ctx:
        cmd = group.get_command(ctx, "list")
    assert cmd.name == "list"
    assert not hasattr(cmd, "_lazy_loader")  # real command, not a placeholder
    with click.Context(group) as ctx:
        assert group.get_command(ctx, "list") is cmd  # cached


def test_get_command_returns_none_for_unknown():
    group = _fresh_group()
    with click.Context(group) as ctx:
        assert group.get_command(ctx, "nope") is None


def test_package_lazy_attribute_access():
    import gwasstudio.cli as cli_pkg

    assert cli_pkg.list_projects.name == "list"
    assert cli_pkg.query_metadata.name == "meta-query"
    assert cli_pkg.export.name == "export"


def test_main_help_lists_all_commands():
    from click.testing import CliRunner

    group = _fresh_group()
    result = CliRunner().invoke(group, ["--help"])
    assert result.exit_code == 0
    for name in ("export", "info", "ingest", "list", "meta-query"):
        assert name in result.output


def test_utils_reexports_context_and_tableio_helpers():
    from gwasstudio.cli import context, table_io, utils

    assert utils.create_config_from_context is context.create_config_from_context
    assert utils._resolve_s3_config is context._resolve_s3_config
    assert utils.load_yaml_file is context.load_yaml_file
    assert utils.parse_uri is context.parse_uri
    assert utils.mongo_conn_info is context.mongo_conn_info
    assert utils.write_table is table_io.write_table
    assert utils.write_if_not_empty is table_io.write_if_not_empty
