import unittest
import os
import numpy as np

from sentinelhub.io_utils import read_data


class TestIO(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.working_dir = os.path.dirname(os.path.realpath(__file__))

    def test_img_read(self):
        W, H = 2048, 2048
        exts = frozenset(['tif', 'jpg', 'png'])
        for ext in exts:
            with self.subTest(msg=ext):
                filepath = '{}/TestInputs/ml.{}'.format(self.working_dir, ext)
                img = read_data(filepath)
                w, h, d = np.shape(img)
                self.assertEqual(w, W, "Width mismatch")
                self.assertEqual(h, H, "Height mismatch")
                self.assertEqual(d, 3, "Expected RGB")


if __name__ == '__main__':
    unittest.main()
