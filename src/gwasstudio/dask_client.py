from contextlib import contextmanager

from dask.distributed import Client
from dask.distributed import LocalCluster
from dask_gateway import Gateway
from dask_jobqueue import SLURMCluster as Cluster

from gwasstudio import logger
from gwasstudio.utils.cfg import get_dask_config

dask_deployment_types = ["local", "gateway", "slurm"]


@contextmanager
def manage_daskcluster(ctx):
    dask_ctx = get_dask_config(ctx)
    cluster = DaskCluster(**dask_ctx)
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
        _address = kwargs.get("address")
        _image = kwargs.get("image")
        _cores = kwargs.get("cores_per_worker")
        _workers = kwargs.get("workers")
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
                cluster.scale(_workers)  # Minimum number of workers
                logger.info(
                    f"Dask cluster: starting {_workers} workers, with {_mem} of memory and {_cores} cpus per worker and address {_address}"
                )
                logger.info("Connecting to Dask scheduler...")
                try:
                    self.client = cluster.get_client() # Connect to that cluster
                except Exception as e:
                    logger.error("Failed to get Dask client", exc_info=True)
                    raise
                logger.info("Waiting for Dask workers to become available...")
                self.client.wait_for_workers(n_workers=_workers, timeout=120)
                worker_info = self.client.scheduler_info().get("workers", {})
                if not worker_info:
                    logger.error("No workers connected after waiting. Cluster is likely misconfigured or failing to start.")
                    raise RuntimeError("No workers connected")
                else:
                    logger.info(f"Workers ready: {len(worker_info)}")
                
                self.type_cluster = type(cluster)

                try:
                    self.client.run_on_scheduler(
                        lambda dask_scheduler: dask_scheduler.log_event("HEARTBEAT", "Dask scheduler is alive")
                    )
                    logger.debug("Successfully sent heartbeat log event to Dask scheduler.")
                except Exception as e:
                    logger.warning("Could not log heartbeat event to Dask scheduler", exc_info=True)
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
            cluster.scale(_workers)
            logger.info(
                f"Dask SLURM cluster: starting {_workers} workers, with {_mem} of memory and {_cores} cpus per worker"
            )
            self.client = Client(cluster)  # Connect to that cluster
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