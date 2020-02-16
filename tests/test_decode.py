import unittest
import os

import numpy as np

from sentinelhub import CRS, MimeType, TestSentinelHub
from sentinelhub.decoding import decode_tar


class TestDecode(TestSentinelHub):
    def test_tar(self):
        tar_path = os.path.join(self.INPUT_FOLDER, 'img.tar')
        with open(tar_path, 'rb') as tar_file:
            tar_bytes = tar_file.read()

        tar_dict = decode_tar(tar_bytes)
        image, metadata = tar_dict['default.tif'], tar_dict['userdata.json']

        self.assertIsInstance(image, np.ndarray)
        self.assertEqual(image.shape, (856, 512, 3))

        self.assertIn('norm_factor', metadata)
        self.assertEqual(metadata['norm_factor'], 0.0001)


if __name__ == "__main__":
    unittest.main()
