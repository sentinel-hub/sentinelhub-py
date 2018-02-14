import unittest
import numpy as np
from tests_all import TestSentinelHub

from sentinelhub.io_utils import read_data


class TestIO(TestSentinelHub):

    def test_img_read(self):
        W, H = 2048, 2048
        exts = frozenset(['tif', 'jpg', 'png', 'jp2'])
        for ext in exts:
            with self.subTest(msg=ext):
                filepath = '{}/ml.{}'.format(self.INPUT_FOLDER, ext)
                img = read_data(filepath)
                self.assertEqual(np.shape(img), (W, H, 3) if ext != 'jp2' else (343, 343, 3),
                                 "Image dimension mismatch")


if __name__ == '__main__':
    unittest.main()
