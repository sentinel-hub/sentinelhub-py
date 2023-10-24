from __future__ import annotations

import os

import numpy as np
import pytest
from requests import Response

from sentinelhub.decoding import decode_sentinelhub_err_msg, decode_tar


def test_tar(input_folder: str) -> None:
    tar_path = os.path.join(input_folder, "img.tar")
    with open(tar_path, "rb") as tar_file:
        tar_bytes = tar_file.read()

    tar_dict = decode_tar(tar_bytes)
    image, metadata = tar_dict["default.tif"], tar_dict["userdata.json"]

    assert isinstance(image, np.ndarray)
    assert image.shape == (856, 512, 3)

    assert "norm_factor" in metadata
    assert metadata["norm_factor"] == 0.0001


HTML_RESPONSE = (
    '<html>\n<head>\n<meta http-equiv="Content-Type" content="text/html;charset=utf-8"/>\n<title>Error 500 Request'
    " failed.</title>\n</head>\n<body><h2>HTTP ERROR 500</h2>\n<p>Problem accessing /oauth/tokeninfo. Reason:\n<pre>"
    " Request failed.</pre></p>\n</body>\n</html>\n"
)
PARSED_HTML = "HTTP ERROR 500 Problem accessing /oauth/tokeninfo. Reason: Request failed."


@pytest.mark.parametrize(
    ("content", "expected_message"),
    [
        (None, ""),
        (False, ""),
        ("Text message!", "Text message!"),
        ('{"error": "Json message!"}', "Json message!"),
        ('{ "foo":  {"bar"  : 42}\n }', '{"foo": {"bar": 42}}'),
        (HTML_RESPONSE, PARSED_HTML),
    ],
)
def test_decode_sentinelhub_err_msg(content: str | bool | None, expected_message: str) -> None:
    response = Response()
    response._content = content.encode() if isinstance(content, str) else content  # noqa: SLF001

    decoded_message = decode_sentinelhub_err_msg(response)
    assert decoded_message == expected_message
