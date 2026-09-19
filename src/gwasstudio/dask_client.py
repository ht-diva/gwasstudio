import datetime
import json
import os
import subprocess
from contextlib import contextmanager
from pathlib import Path

from dask.distributed import Client, LocalCluster
from dask_gateway import Gateway
from dask_jobqueue import SLURMCluster as Cluster
from platformdirs import user_config_dir

from gwasstudio import logger
from gwasstudio.core.config import GWASStudioConfig

dask_deployment_types = ["local", "gateway", "slurm"]

# Cluster state directory using platformdirs
cluster_state_dir = Path(user_config_dir("gwasstudio", appauthor=False)) / "clusters"
cluster_state_dir.mkdir(parents=True, exist_ok=True)


def _config_to_dict(config: GWASStudioConfig) -> dict:
    """Convert a GWASStudioConfig DaskConfig to a plain dict for DaskCluster."""
    return {
        "deployment": config.dask.deployment,
        "workers": config.dask.workers,
        "cores_per_worker": config.dask.cores_per_worker,
        "memory_per_worker": config.dask.memory_per_worker,
        "interface": config.dask.interface,
        "gw_address": config.dask.gw_address,
        "gw_image": config.dask.gw_image,
        "walltime": config.dask.walltime,
        "job_script_prologue": config.dask.job_script_prologue,
        "python": config.dask.python,
        "local_directory": config.dask.local_directory,
    }


@contextmanager
def manage_daskcluster(config: GWASStudioConfig):
    """
    Manage a Dask cluster lifecycle for ingestion/export.

    Creates the appropriate cluster (local/gateway/slurm) based on
    ``config.dask.deployment``, yields the connected Client, and
    ensures clean shutdown on exit.

    Args:
        config: GWASStudioConfig containing Dask settings.
    """
    dask_kwargs = _config_to_dict(config)
    cluster = DaskCluster(**dask_kwargs)
    client = cluster.get_connected_client()
    logger.debug(f"Dask client: {client}")
    logger.info(f"Dask cluster dashboard: {cluster.dashboard_link}")
    try:
        yield client
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise
    finally:
        cluster.shutdown()


# config in $HOME/.config/dask/jobqueue.yaml
class DaskCluster:
    def __init__(self, deployment=None, **kwargs):
        """
        Minimal Dask cluster initializer – only three configuration knobs are used:
        * ``workers`` – total number of workers to launch
        * ``cores_per_worker`` – CPU cores allocated per worker
        * ``memory_per_worker`` – memory allocated per worker (string accepted by Dask)
        """
        _address = kwargs.get("gw_address")
        _image = kwargs.get("gw_image")
        _cores = kwargs.get("cores_per_worker")
        _workers = kwargs.get("workers", 1)
        _mem = kwargs.get("memory_per_worker")
        _interface = kwargs.get("interface")
        _walltime = kwargs.get("walltime")
        _job_script_prologue = kwargs.get("job_script_prologue", [])
        if isinstance(_job_script_prologue, str):
            _job_script_prologue = [line.strip() for line in _job_script_prologue.split(",")]
        _python = kwargs.get("python")
        _local_directory = kwargs.get("local_directory")

        if deployment == "gateway":
            if _address:
                try:
                    if isinstance(_mem, str) and _mem.lower().endswith("gib"):
                        _mem = float(_mem[:-3])
                    else:
                        _mem = float(_mem)
                except Exception as e:
                    raise ValueError(f"Invalid format for --memory_per_worker: {_mem}") from e

                self.gateway = Gateway(address=_address)
                options = self.gateway.cluster_options()
                options.worker_cores = _cores  # Cores per worker
                options.worker_memory = _mem  # Memory per worker
                options.image = _image  # Worker image

                # Create a cluster
                cluster = self.gateway.new_cluster(options)

                # Scale the cluster
                cluster.scale(_workers)
                logger.info(
                    f"Dask cluster: starting {_workers} workers, with {_mem} of memory and {_cores} cpus per worker and address {_address}"
                )

                logger.info("Connecting to Dask scheduler...")
                self.client = cluster.get_client()
                logger.info("Waiting for Dask workers to become available...")
                try:
                    self.client.wait_for_workers(n_workers=_workers, timeout=120)
                except TimeoutError:
                    logger.error("Timeout: Dask Gateway workers did not start within 120 seconds")
                    raise RuntimeError("Dask Gateway workers timeout. Cluster may be overloaded")
                logger.info(f"Workers ready: {len(self.client.scheduler_info()['workers'])}")
                self.type_cluster = type(cluster)
            else:
                raise ValueError("Address must be provided for gateway deployment")

        elif deployment == "slurm":
            # https://jobqueue.dask.org/en/latest/clusters-configuration-setup.html#processes
            processes = self.divide_and_round(_cores, divider=3)  # one process per three cores
            cluster = Cluster(
                cores=_cores,
                interface=_interface,
                job_script_prologue=_job_script_prologue,
                local_directory=_local_directory,
                memory=_mem,
                processes=processes,
                python=_python,
                walltime=_walltime,
            )
            logger.debug(cluster.job_script())
            cluster.scale(jobs=_workers)
            logger.info(
                f"Dask SLURM cluster: starting {_workers} workers, with {_mem} of memory and {_cores} cpus per worker"
            )

            logger.info("Connecting to Dask scheduler...")
            self.client = Client(cluster)  # Connect to that cluster
            logger.info("Waiting for Dask workers to become available...")
            self.client.wait_for_workers(n_workers=_workers, timeout=None)  # wait indefinitely
            logger.info(f"Workers ready: {len(self.client.scheduler_info()['workers'])}")
            self.type_cluster = type(cluster)

        elif deployment == "local":
            cluster = LocalCluster(
                n_workers=_workers,
                threads_per_worker=_cores,
                memory_limit=_mem,
            )
            self.client = Client(cluster)
            self.type_cluster = type(cluster)
            logger.info(
                f"Dask local cluster: starting {_workers} workers, with {_mem} of memory and {_cores} cpus per worker"
            )

        else:
            raise ValueError("Invalid dask_deployment option. Please choose from 'gateway', 'slurm', or 'local'.")

        self.dashboard = self.client.dashboard_link

    @property
    def dashboard_link(self):
        if self.dashboard is None:
            raise ValueError("Dashboard link is not available. Please start the cluster first.")
        return self.dashboard

    @staticmethod
    def divide_and_round(number: int, divider: int = 3) -> int:
        """
        Divides a given number by a specified divider and rounds the result to the nearest integer.
        Ensures the result is not less than 1.

        Args:
            number (int): The number to be divided.
            divider (int, optional): The divider. Defaults to 3.

        Returns:
            int: The rounded result, guaranteed to be at least 1.
        """
        result = round(number / divider)
        return max(result, 1)

    @staticmethod
    def slurm_wait_time(job_id: int, default_timeout: int = 120) -> int:
        """
        Estimate wait time in seconds for a SLURM job ID using `squeue`.
        If job is PENDING, calculates and returns wait time in seconds.
        If job is RUNNING, returns default timeout.

        Args:
            job_id (int): The SLURM job ID
            default_timeout (int): The fallback wait time in seconds
        Returns:
            int: The SLURM queueing time in seconds
        """
        try:
            start_result = subprocess.run(
                ["squeue", "--start", "-j", str(job_id), "--noheader"], capture_output=True, text=True, check=True
            )
            start_line = start_result.stdout.strip()
            if not start_line:
                return default_timeout  # Job already RUNNING

            start_time = datetime.datetime.strptime(start_line.split()[-1], "%Y-%m-%dT%H:%M:%S")
            wait_seconds = int((start_time - datetime.datetime.now()).total_seconds())
            return max(wait_seconds, 0)

        except Exception as e:
            logger.warning(f"Failed to estimate SLURM queue wait: {e}")
            return default_timeout

    def get_connected_client(self):
        return self.client

    def get_type_cluster(self):
        return self.type_cluster

    def shutdown(self):
        if self.client:
            logger.info("Shutting down Dask client and cluster.")
            self.client.close()  # Close the client

        if hasattr(self, "gateway") and self.gateway:
            logger.info("Closing Dask Gateway session.")
            self.gateway.close()  # Close the Dask Gateway


class ClusterStateManager:
    """
    Manage persistent Dask cluster state across gwasstudio sessions.

    Uses Dask's built-in connection file format for client persistence
    and a JSON metadata file for tracking cluster configuration.
    """

    @staticmethod
    def _get_connection_file_path(name: str = "default") -> Path:
        """Get path to Dask connection file for a named cluster."""
        return cluster_state_dir / f"{name}.json"

    @staticmethod
    def _get_metadata_file_path(name: str = "default") -> Path:
        """Get path to metadata file for a named cluster."""
        return cluster_state_dir / f"{name}.metadata.json"

    @staticmethod
    def start_cluster(name: str = "default", config: GWASStudioConfig = None, **kwargs) -> DaskCluster:
        """
        Start a persistent Dask cluster and save its state.

        Args:
            name: Cluster profile name (default: "default")
            config: GWASStudioConfig with Dask settings
            **kwargs: Override config.dask settings

        Returns:
            DaskCluster: The started cluster instance
        """
        # Build configuration
        if config is None:
            config = GWASStudioConfig()

        dask_kwargs = _config_to_dict(config)
        dask_kwargs.update(kwargs)

        # Start the cluster
        cluster = DaskCluster(**dask_kwargs)

        # Save connection file (scheduler file)
        connection_file = ClusterStateManager._get_connection_file_path(name)
        cluster.client.write_scheduler_file(str(connection_file))

        # Save metadata
        metadata = {
            "name": name,
            "deployment": dask_kwargs.get("deployment"),
            "workers": dask_kwargs.get("workers"),
            "cores_per_worker": dask_kwargs.get("cores_per_worker"),
            "memory_per_worker": dask_kwargs.get("memory_per_worker"),
            "dashboard_link": cluster.dashboard_link,
            "connection_file": str(connection_file),
            "created_at": datetime.datetime.now().isoformat(),
        }
        metadata_file = ClusterStateManager._get_metadata_file_path(name)
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"Cluster '{name}' started and state saved to {cluster_state_dir}")
        return cluster

    @staticmethod
    def get_cluster(name: str = "default") -> DaskCluster | None:
        """
        Get an existing cluster by name.

        Args:
            name: Cluster profile name

        Returns:
            DaskCluster if running, None otherwise
        """
        connection_file = ClusterStateManager._get_connection_file_path(name)
        metadata_file = ClusterStateManager._get_metadata_file_path(name)

        if not connection_file.exists() or not metadata_file.exists():
            logger.debug(f"No saved cluster state found for '{name}'")
            return None

        # Load metadata
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        # Try to connect using the saved scheduler file
        try:
            client = Client(scheduler_file=str(connection_file))
            dashboard_link = metadata.get("dashboard_link")

            # Create a DaskCluster wrapper around the existing client
            cluster = DaskCluster.__new__(DaskCluster)
            cluster.client = client
            cluster.dashboard = dashboard_link
            cluster.type_cluster = None  # Not available from connection file

            logger.debug(f"Connected to existing cluster '{name}'")
            return cluster

        except Exception as e:
            logger.warning(f"Failed to connect to cluster '{name}': {e}. Cluster may have been shut down.")
            # Clean up stale state
            ClusterStateManager.stop_cluster(name, cleanup=True)
            return None

    @staticmethod
    def stop_cluster(name: str = "default", cleanup: bool = False):
        """
        Stop a running cluster and remove its state.

        Args:
            name: Cluster profile name
            cleanup: If True, remove state files even if stop fails
        """
        connection_file = ClusterStateManager._get_connection_file_path(name)
        metadata_file = ClusterStateManager._get_metadata_file_path(name)

        if not connection_file.exists() and not metadata_file.exists():
            logger.debug(f"No cluster state found for '{name}'")
            return

        # Try to get and shutdown the cluster
        cluster = ClusterStateManager.get_cluster(name)
        if cluster:
            try:
                cluster.shutdown()
                logger.info(f"Cluster '{name}' stopped successfully")
            except Exception as e:
                logger.warning(f"Error stopping cluster '{name}': {e}")

        # Remove state files
        if cleanup or cluster:
            for f in [connection_file, metadata_file]:
                if f.exists():
                    f.unlink()
                    logger.debug(f"Removed {f}")

    @staticmethod
    def list_clusters() -> list[dict]:
        """
        List all saved cluster profiles.

        Returns:
            List of metadata dicts for each saved cluster
        """
        clusters = []
        for pattern in cluster_state_dir.glob("*.metadata.json"):
            try:
                with open(pattern, "r") as f:
                    metadata = json.load(f)
                    # Check if connection file still exists
                    connection_file = cluster_state_dir / f"{metadata['name']}.json"
                    metadata["connected"] = connection_file.exists()
                    # Try to check if actually running
                    try:
                        client = Client(scheduler_file=str(connection_file), timeout=1)
                        metadata["running"] = True
                        client.close()
                    except Exception:
                        metadata["running"] = False
                    clusters.append(metadata)
            except Exception as e:
                logger.warning(f"Error reading cluster metadata from {pattern}: {e}")
        return clusters

    @staticmethod
    def get_cluster_info(name: str = "default") -> dict | None:
        """
        Get detailed information about a specific cluster.

        Args:
            name: Cluster profile name

        Returns:
            Dict with cluster info, or None if not found
        """
        cluster = ClusterStateManager.get_cluster(name)
        if cluster is None:
            return None

        metadata_file = ClusterStateManager._get_metadata_file_path(name)
        if not metadata_file.exists():
            return None

        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        # Add runtime info from client
        try:
            scheduler_info = cluster.client.scheduler_info()
            metadata["workers"] = list(scheduler_info["workers"].keys())
            metadata["worker_count"] = len(scheduler_info["workers"])
        except Exception:
            metadata["workers"] = []
            metadata["worker_count"] = 0

        return metadata

    @staticmethod
    def is_cluster_running(name: str = "default") -> bool:
        """Check if a cluster is currently running."""
        return ClusterStateManager.get_cluster(name) is not None
