import unittest
import os.path
import numpy as np

from sentinelhub import read_data, write_data, TestSentinelHub


class TestIO(TestSentinelHub):

    class IOTestCase:

        def __init__(self, filename, mean, shape=(2048, 2048, 3)):
            self.filename = filename
            self.mean = mean
            self.shape = shape

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.test_cases = [
            cls.IOTestCase('img.tif', 13577.494856),
            cls.IOTestCase('img.jpg', 52.41194),
            cls.IOTestCase('img.png', 52.33736),
            cls.IOTestCase('img-8bit.jp2', 47.09060, (343, 343, 3)),
            cls.IOTestCase('img-15bit.jp2', 0.3041897, (1830, 1830)),
            cls.IOTestCase('img-16bit.jp2', 0.3041897, (1830, 1830)),
        ]

    def test_img_read(self):

        for test_case in self.test_cases:
            with self.subTest(msg=test_case.filename):
                file_path = os.path.join(self.INPUT_FOLDER, test_case.filename)
                img = read_data(file_path)

                self.assertEqual(img.shape, test_case.shape,
                                 'Expected shape {}, got {}'.format(test_case.shape, img.shape))
                self.assertAlmostEqual(np.mean(img), test_case.mean, delta=1e-4,
                                       msg='Expected mean {}, got {}'.format(test_case.mean, np.mean(img)))

                new_file_path = os.path.join(self.OUTPUT_FOLDER, test_case.filename)
                write_data(new_file_path, img)
                new_img = read_data(new_file_path)

                if not test_case.filename.endswith('jpg'):
                    self.assertTrue(np.array_equal(img, new_img), msg="Original and new image are not the same")


if __name__ == '__main__':
    unittest.main()
