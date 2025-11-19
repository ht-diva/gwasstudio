from unittest.mock import patch

import pytest

# Import the function we want to test
from gwasstudio.utils.cfg import get_dask_batch_size


# Helper to build a fake Dask config dictionary
def fake_dask_config(workers=4, cores_per_worker=2, batch_size=32):
    return {
        "workers": workers,
        "cores_per_worker": cores_per_worker,
        "batch_size": batch_size,
    }


@pytest.mark.parametrize(
    "capacity_mode, expected",
    [
        (False, 32),  # regular batch size from config
        (True, 4 * 2),  # workers * cores_per_worker
    ],
)
def test_get_dask_batch_size(capacity_mode, expected):
    """Validate that `get_dask_batch_size` returns the correct value
    depending on the `capacity_mode` flag."""
    fake_ctx = object()  # the actual value isn’t used; it’s just passed through

    # Patch `get_dask_config` used inside the function so it returns our fake dict
    with patch("gwasstudio.utils.cfg.get_dask_config", return_value=fake_dask_config()):
        result = get_dask_batch_size(fake_ctx, capacity_mode=capacity_mode)

    assert result == expected
