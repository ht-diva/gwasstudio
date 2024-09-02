import dask
from dask.distributed import Client
from dask_jobqueue import SLURMCluster as Cluster
from gwasstudio import logger


# config in $HOME/.config/dask/jobqueue.yaml
class DaskClient:
    def __init__(self, **kwargs):
        _min = kwargs.get("minimum_workers")
        _max = kwargs.get("maximum_workers")
        _mem = kwargs.get("memory_workers")
        _cpu = kwargs.get("cpu_workers")
        logger.info(f"Dask cluster: starting from {_min} to {_max} workers, {_mem} of memory and {_cpu} cpus per worker")
        cluster = Cluster(memory = _mem, cores = _cpu)

        cluster.adapt(minimum=_min, maximum=_max)
        client = Client(cluster)  # Connect to that cluster
        
        self.dashboard = ""  # client.dashboard_link

        # dask.config.set({"dataframe.convert-string": False})

    def get_dashboard(self):
        return self.dashboard
