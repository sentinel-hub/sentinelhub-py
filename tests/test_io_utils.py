import unittest
import numpy as np
from tests_all import TestSentinelHub

from sentinelhub.io_utils import read_data


class TestIO(TestSentinelHub):

    def test_img_read(self):
        W, H = 2048, 2048
        formats = [('tif', 13577.49), ('jpg', 52.41), ('png', 52.34), ('jp2', 47.09)]
        for ext, exp_mean in formats:
            with self.subTest(msg=ext):
                filepath = '{}/ml.{}'.format(self.INPUT_FOLDER, ext)
                img = read_data(filepath)
                self.assertEqual(np.shape(img), (W, H, 3) if ext != 'jp2' else (343, 343, 3),
                                 "{} image dimension mismatch".format(ext))
                self.assertAlmostEqual(np.mean(img), exp_mean, delta=1e-1,
                                       msg="{} image has incorrect values".format(ext))


if __name__ == '__main__':
    unittest.main()
