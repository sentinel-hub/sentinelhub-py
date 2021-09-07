import os
from platform import python_implementation

import numpy as np
import pytest
from pytest import approx

from sentinelhub import read_data, write_data


@pytest.mark.parametrize('filename, mean, shape', [
    ('img.tif', 13577.494856, (2048, 2048, 3)),
    ('img.jpg', 52.41194, (2048, 2048, 3)),
    ('img.png', 52.33736, (2048, 2048, 3)),
    ('img-8bit.jp2', 47.09060, (343, 343, 3)),
    ('img-15bit.jp2', 0.3041897, (1830, 1830)),
    ('img-16bit.jp2', 0.3041897, (1830, 1830)),
])
def test_img_read(input_folder, output_folder, filename, mean, shape):
    img = read_data(os.path.join(input_folder, filename))

    assert img.shape == shape

    if filename != 'img.jpg' or python_implementation() != 'PyPy':
        assert np.mean(img) == approx(mean, abs=1e-4)

    assert img.flags['WRITEABLE'], 'Obtained numpy array is not writeable'

    file_path = os.path.join(output_folder, filename)
    write_data(file_path, img)
    new_img = read_data(file_path)

    if not filename.endswith('jpg'):
        assert np.array_equal(img, new_img), 'Original and saved image are not the same'


def test_read_tar_with_folder(input_folder):
    path = os.path.join(input_folder, 'tar-folder.tar')
    data = read_data(path)

    assert data == {'tar-folder/simple.json': {'message': 'test'}}
