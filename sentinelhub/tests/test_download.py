import pytest

from sentinelhub import download

def test_download():
    assert download("UTM31") == "Downloading UTM31"

def test_no_input():
    with pytest.raises(TypeError):
        download()
