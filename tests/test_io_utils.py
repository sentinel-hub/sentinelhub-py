import unittest
import numpy as np
from tests_all import TestSentinelHub

from sentinelhub.io_utils import read_data


class TestIO(TestSentinelHub):

    def test_img_read(self):
        W, H = 2048, 2048
        exts = frozenset(['tif', 'jpg', 'png'])
        for ext in exts:
            with self.subTest(msg=ext):
                filepath = '{}/ml.{}'.format(self.INPUT_FOLDER, ext)
                img = read_data(filepath)
                w, h, d = np.shape(img)
                self.assertEqual(w, W, "Width mismatch")
                self.assertEqual(h, H, "Height mismatch")
                self.assertEqual(d, 3, "Expected RGB")


if __name__ == '__main__':
    unittest.main()
