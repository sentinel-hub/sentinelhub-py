"""
Tests for sh_utils.py module
"""
import math

import pytest

from sentinelhub import DownloadClient
from sentinelhub.sh_utils import FeatureIterator


class DummyIterator(FeatureIterator):
    """ As features it generates integer values
    """
    def __init__(self, total, limit):
        """
        :param total: Number of features in total
        :param limit: Max number of features provided by each fetch
        """
        self.total = total
        self.limit = limit

        self.feature_fetch_count = 0
        super().__init__(client=DownloadClient(), url='')

    def _fetch_features(self):
        start_interval = len(self.features)
        end_interval = min(start_interval + self.limit, self.total)

        new_features = list(range(start_interval, end_interval))

        if not new_features:
            self.finished = True
        else:
            self.feature_fetch_count += 1

        return new_features


@pytest.mark.parametrize('total,limit', [
    (100, 1000),
    (100, 10),
    (100, 7),
    (100, 1)
])
def test_feature_iterator(total, limit):
    iterator = DummyIterator(total, limit)

    for _ in range(2):
        features = list(iterator)
        assert features == list(range(total))
        assert iterator.feature_fetch_count == math.ceil(total / limit)

    iterator = iter(iterator)
    assert isinstance(iterator, DummyIterator)

    for idx in range(8):
        value = next(iterator)
        assert value == idx
