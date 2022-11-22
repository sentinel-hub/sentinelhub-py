from typing import Dict

import numpy as np
import pytest

from sentinelhub.testing_utils import assert_statistics_match


@pytest.mark.parametrize("data", (np.arange(100).reshape(10, 10, 1),))
@pytest.mark.parametrize(
    "expected_statistics, succeed",
    (
        [
            {"exp_shape": (10, 10, 1), "exp_min": 0, "exp_max": 99, "exp_mean": 49.5, "exp_median": 49.5},
            True,
        ],
        [
            {"exp_shape": (10, 10, 1), "exp_min": 0, "exp_max": 99, "exp_mean": 50, "exp_median": 50, "delta": 0.05},
            True,
        ],
        [
            {
                "exp_shape": (10, 10, 1),
                "exp_min": 0,
                "exp_max": 99,
                "exp_mean": 50,
                "exp_median": 50,
                "delta": 0.05,
                "abs_delta": True,
            },
            False,
        ],
    ),
)
def test_assert_statistics_match(data: np.array, expected_statistics: Dict, succeed: bool) -> None:
    if succeed:
        assert_statistics_match(data, **expected_statistics)
    else:
        with pytest.raises(AssertionError):
            assert_statistics_match(data, **expected_statistics)
