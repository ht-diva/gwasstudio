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
    client = cluster.get_client()
    logger.debug(f"Dask client: {client}")
    logger.info(f"Dask cluster dashboard: {cluster.dashboard_link}")
    try:
        yield
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise
    finally:
        cluster.shutdown()


# config in $HOME/.config/dask/jobqueue.yaml
class DaskCluster:
    def __init__(self, deployment=None, **kwargs):
        _address = kwargs.get("address")
        _cores = kwargs.get("cpu_workers")
        _workers = kwargs.get("num_workers")
        _mem = kwargs.get("memory_workers")
        _workers_local = kwargs.get("local_workers")
        _threads_local = kwargs.get("local_threads")
        _threads_memory = kwargs.get("local_memory")
        _walltime = kwargs.get("walltime")

        if deployment == "gateway":
            if _address:
                gateway = Gateway(address=_address, auth="kerberos")
                options = gateway.cluster_options()
                options.worker_cores = _cores  # Cores per worker
                options.worker_memory = _mem  # Memory per worker
                options.worker_walltime = _walltime  # Time limit for each worker

                # Create a cluster
                cluster = gateway.new_cluster(options)

                # Scale the cluster
                cluster.scale(_workers)  # Minimum number of workers
                logger.info(
                    f"Dask cluster: starting {_workers} workers, with {_mem} of memory and {_cores} cpus per worker and address {_address}"
                )
                self.client = Client(cluster)  # Connect to that cluster
                self.type_cluster = type(cluster)
            else:
                raise ValueError("Address must be provided for gateway deployment")

        elif deployment == "slurm":
            # https://jobqueue.dask.org/en/latest/clusters-configuration-setup.html#processes
            processes = self.divide_and_round(_cores, divider=3)  # one process per three cores
            cluster = Cluster(memory=_mem, cores=_cores, processes=processes, walltime=_walltime)
            cluster.scale(_workers)
            logger.info(
                f"Dask SLURM cluster: starting {_workers} workers, with {_mem} of memory and {_cores} cpus per worker"
            )
            self.client = Client(cluster)  # Connect to that cluster
            self.type_cluster = type(cluster)

        elif deployment == "local":
            cluster = LocalCluster(
                n_workers=_workers_local,
                threads_per_worker=_threads_local,
                memory_limit=_threads_memory,
            )
            self.client = Client(cluster)
            self.type_cluster = type(cluster)
            logger.info(
                f"Dask local cluster: starting {_workers_local} workers, with {_threads_memory} of memory and {_threads_local} cpus per worker"
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

    def get_client(self):
        return self.client

    def get_type_cluster(self):
        return self.type_cluster

    def shutdown(self):
        if self.client:
            logger.info("Shutting down Dask client and cluster.")
            self.client.close()  # Close the client
