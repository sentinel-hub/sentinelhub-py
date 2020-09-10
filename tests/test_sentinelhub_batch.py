"""
A module that tests an interface for Sentinel Hub Batch processing
"""
import unittest

from sentinelhub import SentinelHubBatch, TestSentinelHub


class TestSentinelHubBatchTilingGrid(TestSentinelHub):
    """ Tests for the the tiling grid endpoint
    """
    def test_iter_tiling_grids(self):
        tiling_grids = list(SentinelHubBatch.iter_tiling_grids(config=self.CONFIG))

        self.assertTrue(len(tiling_grids) >= 1)
        self.assertTrue(all(isinstance(item, dict) for item in tiling_grids))

    def test_single_tiling_grid(self):
        tiling_grid = SentinelHubBatch.get_tiling_grid(0, config=self.CONFIG)

        self.assertTrue(isinstance(tiling_grid, dict))


if __name__ == '__main__':
    unittest.main()
