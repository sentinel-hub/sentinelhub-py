import unittest
import os.path
import numpy as np
from tests_all import TestSentinelHub

from sentinelhub import read_data, write_data


class TestIO(TestSentinelHub):

    def test_img_read(self):
        W, H = 2048, 2048
        formats = [('tif', 13577.49), ('jpg', 52.41), ('png', 52.34), ('jp2', 47.09)]
        for ext, exp_mean in formats:
            with self.subTest(msg=ext):
                filename = os.path.join(self.INPUT_FOLDER, 'ml.{}'.format(ext))
                img = read_data(filename)
                self.assertEqual(np.shape(img), (W, H, 3) if ext != 'jp2' else (343, 343, 3),
                                 "{} image dimension mismatch".format(ext))
                self.assertAlmostEqual(np.mean(img), exp_mean, delta=1e-1,
                                       msg="{} image has incorrect values".format(ext))

                new_filename = os.path.join(self.OUTPUT_FOLDER, os.path.split(filename)[-1])
                write_data(new_filename, img)
                new_img = read_data(new_filename)
                if ext != 'jpg':
                    self.assertTrue(np.array_equal(img, new_img), msg="Original and new image are not the same")


if __name__ == '__main__':
    unittest.main()
