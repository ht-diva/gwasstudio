import click
import cloup

from gwasstudio import logger
from gwasstudio.core.config import DaskConfig, GWASStudioConfig
from gwasstudio.dask_client import ClusterStateManager, dask_deployment_types

help_doc = """
Manage Dask cluster lifecycle independently from gwasstudio operations.

This allows you to start a cluster once and reuse it across multiple
commands (ingest, export, query), avoiding repeated cluster creation
and teardown overhead.
"""


@cloup.group("cluster", help=help_doc)
@cloup.option(
    "--name",
    "-n",
    default="default",
    help="Cluster profile name (default: 'default')",
)
@click.pass_context
def cluster_group(ctx, name):
    """Cluster management command group."""
    ctx.ensure_object(dict)
    ctx.obj["cluster_name"] = name


@cluster_group.command("start", help="Start a persistent Dask cluster.")
@cloup.option(
    "--deployment",
    type=click.Choice(dask_deployment_types),
    required=True,
    help="Deployment type: local, gateway, or slurm",
)
@cloup.option("--workers", default=2, help="Number of workers to start")
@cloup.option("--cores-per-worker", default=2, help="CPU cores per worker")
@cloup.option("--memory-per-worker", default="4GiB", help="Memory per worker (e.g., 36GiB)")
@cloup.option(
    "--gw-address",
    default=None,
    help="Dask Gateway address (required for gateway deployment)",
)
@cloup.option("--gw-image", default=None, help="Dask Gateway worker image")
@cloup.option(
    "--interface",
    default=None,
    help="Network interface for workers (e.g., ib0)",
)
@cloup.option(
    "--local-directory",
    default=None,
    help="Fast local directory for Dask workers",
)
@cloup.option(
    "--walltime",
    default="12:00:00",
    help="Walltime for each worker (SLURM only)",
)
@cloup.option(
    "--python",
    default=None,
    help="Python executable for Dask workers",
)
@cloup.option(
    "--job-script-prologue",
    default=[],
    help="Commands to add to SLURM script before launching workers",
)
@click.pass_context
def cluster_start(
    ctx,
    deployment,
    workers,
    cores_per_worker,
    memory_per_worker,
    gw_address,
    gw_image,
    interface,
    local_directory,
    walltime,
    python,
    job_script_prologue,
):
    """Start a persistent Dask cluster with the specified configuration."""
    name = ctx.obj.get("cluster_name", "default")

    # Build Dask configuration
    dask_config = DaskConfig(
        deployment=deployment,
        workers=workers,
        cores_per_worker=cores_per_worker,
        memory_per_worker=memory_per_worker,
        gw_address=gw_address,
        gw_image=gw_image,
        interface=interface,
        local_directory=local_directory,
        walltime=walltime,
        python=python,
        job_script_prologue=job_script_prologue,
    )
    config = GWASStudioConfig(dask=dask_config)

    # Validate gateway deployment has address
    if deployment == "gateway" and not gw_address:
        click.echo("Error: --gw-address is required for gateway deployment", err=True)
        raise click.Abort()

    click.echo(f"Starting cluster '{name}' with {deployment} deployment...")

    try:
        cluster = ClusterStateManager.start_cluster(
            name=name,
            config=config,
        )
        click.echo(f"Cluster '{name}' started successfully!")
        click.echo(f"  Deployment: {deployment}")
        click.echo(f"  Workers: {workers}")
        click.echo(f"  Cores per worker: {cores_per_worker}")
        click.echo(f"  Memory per worker: {memory_per_worker}")
        click.echo(f"  Dashboard: {cluster.dashboard_link}")
        click.echo(f"\nScheduler file saved to: {ClusterStateManager._get_connection_file_path(name)}")
        click.echo("\nUse 'gwasstudio cluster status' to check cluster status.")
        click.echo("Use 'gwasstudio cluster stop' to shut it down.")
    except Exception as e:
        logger.error(f"Failed to start cluster: {e}")
        raise click.ClickException(f"Failed to start cluster: {e}")


@cluster_group.command("stop", help="Stop a running Dask cluster.")
@cloup.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Force stop and cleanup state files even if error occurs",
)
@click.pass_context
def cluster_stop(ctx, force):
    """Stop a running Dask cluster and clean up its state."""
    name = ctx.obj.get("cluster_name", "default")

    if not ClusterStateManager.is_cluster_running(name):
        # Check if state files exist
        connection_file = ClusterStateManager._get_connection_file_path(name)
        metadata_file = ClusterStateManager._get_metadata_file_path(name)
        if not connection_file.exists() and not metadata_file.exists():
            click.echo(f"No cluster '{name}' found.")
            return
        else:
            click.echo(f"Cluster '{name}' is not running (state files exist but not connected).")

    click.echo(f"Stopping cluster '{name}'...")
    ClusterStateManager.stop_cluster(name, cleanup=force)
    click.echo(f"Cluster '{name}' stopped and state cleaned up.")


@cluster_group.command("status", help="Show status of a Dask cluster.")
@click.pass_context
def cluster_status(ctx):
    """Show detailed status information for a cluster."""
    name = ctx.obj.get("cluster_name", "default")

    info = ClusterStateManager.get_cluster_info(name)
    if info is None:
        click.echo(f"Cluster '{name}' is not running or does not exist.")
        return

    click.echo(f"Cluster: {name}")
    click.echo(f"  Status: {'RUNNING' if info.get('running', False) else 'STOPPED'}")
    click.echo(f"  Deployment: {info.get('deployment', 'N/A')}")
    click.echo(f"  Dashboard: {info.get('dashboard_link', 'N/A')}")
    click.echo(f"  Workers: {info.get('worker_count', 0)}")
    click.echo(f"  Created: {info.get('created_at', 'N/A')}")

    # Show worker details if available
    workers = info.get("workers", [])
    if workers:
        click.echo(f"  Worker addresses:")
        for worker in workers[:5]:  # Show first 5 workers
            click.echo(f"    - {worker}")
        if len(workers) > 5:
            click.echo(f"    ... and {len(workers) - 5} more")


@cluster_group.command("list", help="List all saved cluster profiles.")
def cluster_list():
    """List all saved cluster profiles and their status."""
    clusters = ClusterStateManager.list_clusters()

    if not clusters:
        click.echo("No cluster profiles found.")
        return

    click.echo(f"Found {len(clusters)} cluster profile(s):\n")

    # Create a simple table output
    click.echo(f"{'Name':<20} {'Deployment':<12} {'Status':<10} {'Workers':<8} {'Created':<25}")
    click.echo("-" * 85)

    for cluster in sorted(clusters, key=lambda x: x.get("name", "")):
        name = cluster.get("name", "N/A")
        deployment = cluster.get("deployment", "N/A")
        status = "RUNNING" if cluster.get("running", False) else "STOPPED"
        workers = cluster.get("worker_count", 0)
        created = cluster.get("created_at", "N/A")[:19]  # Truncate to YYYY-MM-DD HH:MM:SS

        click.echo(f"{name:<20} {deployment:<12} {status:<10} {workers:<8} {created:<25}")


@cluster_group.command("cleanup", help="Remove stale cluster state files.")
@cloup.option(
    "--all",
    "-a",
    is_flag=True,
    default=False,
    help="Remove all cluster state files (not just stale ones)",
)
@click.pass_context
def cluster_cleanup(ctx, all):
    """Remove stale cluster state files for clusters that are no longer running."""
    name = ctx.obj.get("cluster_name", "default")

    if all:
        # Clean up all clusters
        clusters = ClusterStateManager.list_clusters()
        for cluster_meta in clusters:
            cluster_name = cluster_meta.get("name", "")
            if cluster_name:
                ClusterStateManager.stop_cluster(cluster_name, cleanup=True)
                click.echo(f"Cleaned up cluster '{cluster_name}'")
        click.echo(f"\nCleaned up all {len(clusters)} cluster profiles.")
    else:
        # Clean up specific cluster
        if ClusterStateManager.is_cluster_running(name):
            click.echo(f"Cluster '{name}' is currently running. Use --all to force cleanup, or stop it first.")
            return

        connection_file = ClusterStateManager._get_connection_file_path(name)
        metadata_file = ClusterStateManager._get_metadata_file_path(name)

        if connection_file.exists() or metadata_file.exists():
            ClusterStateManager.stop_cluster(name, cleanup=True)
            click.echo(f"Cleaned up stale state for cluster '{name}'")
        else:
            click.echo(f"No state files found for cluster '{name}'")
