from dask.distributed import Client
from dask.distributed import LocalCluster
from dask_gateway import Gateway
from dask_jobqueue import SLURMCluster as Cluster

from gwasstudio import logger

dask_deployment_types = ["local", "gateway", "HPC"]


# config in $HOME/.config/dask/jobqueue.yaml
class DaskCluster:
    def __init__(self, dask_deployment=None, **kwargs):
        # _dask_distribute = kwargs.get("dask_distribute")
        _address = kwargs.get("address")
        _cpu_dist = kwargs.get("cpu_workers")
        _min_dist = kwargs.get("minimum_workers")
        _max_dist = kwargs.get("maximum_workers")
        _mem_dist = kwargs.get("memory_workers")
        _workers_local = kwargs.get("local_workers")
        _threads_local = kwargs.get("local_threads")
        _threads_memory = kwargs.get("local_memory")
        _walltime = kwargs.get("walltime")

        if dask_deployment == "gateway":
            if _address:
                gateway = Gateway(address=_address, auth="kerberos")
                options = gateway.cluster_options()
                options.worker_cores = _cpu_dist  # Cores per worker
                options.worker_memory = _mem_dist  # Memory per worker
                options.worker_walltime = _walltime  # Time limit for each worker

                # Create a cluster
                cluster = gateway.new_cluster(options)

                # Scale the cluster
                cluster.scale(_min_dist)  # Minimum number of workers
                cluster.adapt(minimum=_min_dist, maximum=_max_dist)  # Auto-scale between minimum and maximum workers
                logger.info(
                    f"Dask cluster: starting from {_min_dist} to {_max_dist} workers, {_mem_dist} of memory and {_cpu_dist} cpus per worker and address {_address}"
                )
                self.client = Client(cluster)  # Connect to that cluster
                self.type_cluster = type(cluster)
            else:
                raise ValueError("Address must be provided for gateway deployment")

        elif dask_deployment == "slurm":
            cluster = Cluster(memory=_mem_dist, cores=_cpu_dist, processes=1, walltime=_walltime)
            cluster.scale(_min_dist)
            logger.info(
                f"Dask SLURM cluster: starting from {_min_dist} to {_max_dist} workers, {_mem_dist} of memory and {_cpu_dist} cpus per worker"
            )
            self.client = Client(cluster)  # Connect to that cluster
            self.type_cluster = type(cluster)

        elif dask_deployment == "local":
            cluster = LocalCluster(
                n_workers=_workers_local,
                threads_per_worker=_threads_local,
                memory_limit=_threads_memory,
            )
            self.client = Client(cluster)
            self.type_cluster = type(cluster)
            logger.info(
                f"Dask local cluster: starting using {_workers_local} workers, {_threads_memory} of memory and {_threads_local} cpus per worker"
            )

        else:
            raise ValueError("Invalid dask_deployment option. Please choose from 'gateway', 'slurm', or 'local'.")

        self.dashboard = self.client.dashboard_link

    def get_client(self):
        return self.client

    def get_type_cluster(self):
        return self.type_cluster

    def get_dashboard(self):
        print(f"Dashboard available at {self.dashboard}")
        return self.dashboard

    def shutdown(self):
        if self.client:
            logger.info("Shutting down Dask client and cluster.")
            self.client.close()  # Close the client
