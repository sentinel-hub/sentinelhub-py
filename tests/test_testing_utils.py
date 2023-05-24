from typing import Dict

import numpy as np
import pytest

from sentinelhub.testing_utils import assert_statistics_match


@pytest.mark.parametrize(
    ("data", "expected_statistics"),
    [
        (
            np.arange(100).reshape(10, 10, 1),
            {"exp_shape": (10, 10, 1), "exp_min": 0, "exp_max": 99, "exp_mean": 49.5, "exp_median": 49.5},
        ),
        (
            np.arange(100).reshape(10, 10, 1),
            {"exp_shape": (10, 10, 1), "exp_max": 99, "exp_mean": 50, "exp_median": 50, "rel_delta": 0.05},
        ),
        (
            np.arange(100).reshape(10, 10, 1),
            {"exp_shape": (10, 10, 1), "exp_max": 99, "exp_mean": 50, "exp_median": 50, "abs_delta": 0.5},
        ),
    ],
)
def test_assert_statistics_match(data: np.array, expected_statistics: Dict) -> None:
    assert_statistics_match(data, **expected_statistics)


@pytest.mark.parametrize(
    ("data", "expected_statistics"),
    [
        (
            np.arange(100).reshape(10, 10, 1),
            {
                "exp_shape": (10, 10, 1),
                "exp_min": 0,
                "exp_max": 100,
            },
        ),
        (
            np.arange(100).reshape(10, 10, 1),
            {
                "exp_shape": (10, 10, 1),
                "exp_mean": 50,
                "exp_median": 50,
                "abs_delta": 0.05,
            },
        ),
    ],
)
def test_assert_statistics_match_fa(data: np.array, expected_statistics: Dict) -> None:
    with pytest.raises(AssertionError):
        assert_statistics_match(data, **expected_statistics)
