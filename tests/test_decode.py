import os

import numpy as np

from sentinelhub.decoding import decode_tar


def test_tar(input_folder):
    tar_path = os.path.join(input_folder, 'img.tar')
    with open(tar_path, 'rb') as tar_file:
        tar_bytes = tar_file.read()

    tar_dict = decode_tar(tar_bytes)
    image, metadata = tar_dict['default.tif'], tar_dict['userdata.json']

    assert isinstance(image, np.ndarray)
    assert image.shape == (856, 512, 3)

    assert 'norm_factor' in metadata
    assert metadata['norm_factor'] == 0.0001
