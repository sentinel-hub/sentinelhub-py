import json
import re
from typing import List, Optional, Tuple

import pytest

from sentinelhub import DataCollection
from sentinelhub.data_collections_bands import Unit
from sentinelhub.evalscript import generate_evalscript


def parse_evalscript_info(evalscript: str) -> Tuple[List[str], List[str], List[Tuple[str, str, str]]]:
    input_names_re = re.search(r"bands: (\[.*\])", evalscript)
    input_units_re = re.search(r"units: (\[.*\])", evalscript)
    output_spec = re.findall(r'\{ id: "([^\s.]+)", bands: ([^\s.]+), sampleType: "([^\s.]+)" \}', evalscript)

    assert input_names_re is not None, "regex failed for extraction of input names"
    assert input_units_re is not None, "regex failed for extraction of input units"

    input_names = json.loads(input_names_re.group(1))
    input_units = json.loads(input_units_re.group(1))

    return input_names, input_units, output_spec


@pytest.mark.parametrize(
    "data_collection, bands",
    [
        (DataCollection.SENTINEL2_L1C, None),
        (DataCollection.SENTINEL2_L1C, ["B04", "B03", "B02"]),
        (DataCollection.SENTINEL1_IW, ["VV", "VH"]),
    ],
)
def test_input_bands(data_collection: DataCollection, bands: Optional[List[str]]) -> None:
    evalscript = generate_evalscript(data_collection=data_collection, bands=bands)
    input_names, *_ = parse_evalscript_info(evalscript)
    assert len(input_names) == len(bands) if bands is not None else len(data_collection.bands)


@pytest.mark.parametrize(
    "meta_bands",
    [None, ["CLM"], ["CLP", "dataMask"]],
)
def test_input_meta_bands(meta_bands: Optional[List[str]]) -> None:
    data_collection = DataCollection.SENTINEL2_L1C
    evalscript = generate_evalscript(data_collection=data_collection, meta_bands=meta_bands)
    input_names, *_ = parse_evalscript_info(evalscript)

    num_meta_bands = 0 if meta_bands is None else len(meta_bands)
    assert len(input_names) == len(data_collection.bands) + num_meta_bands


@pytest.mark.parametrize("use_dn", [True, False])
@pytest.mark.parametrize(
    "data_collection, bands, meta_bands",
    [
        (DataCollection.SENTINEL2_L1C, ["B04", "B03", "B02"], ["CLP", "sunZenithAngles"]),
        (DataCollection.SENTINEL3_OLCI, ["B04", "B03"], ["HUMIDITY"]),
    ],
)
def test_input_units(data_collection: DataCollection, bands: List[str], meta_bands: List[str], use_dn: bool) -> None:
    evalscript = generate_evalscript(data_collection=data_collection, bands=bands, meta_bands=meta_bands, use_dn=use_dn)
    input_names, input_units, _ = parse_evalscript_info(evalscript)

    band_info_dict = {band_info.name: band_info for band_info in data_collection.bands + data_collection.metabands}
    for name, unit in zip(input_names, input_units):
        band = band_info_dict[name]
        assert Unit(unit) in band.units

        if use_dn and Unit.DN in band.units:
            assert Unit(unit) == Unit.DN


@pytest.mark.parametrize(
    "data_collection, bands, meta_bands",
    [
        (DataCollection.LANDSAT_TM_L2, None, ["BQA"]),
        (DataCollection.SENTINEL2_L2A, ["B04", "B03", "B02"], ["CLP", "SCL"]),
    ],
)
def test_merged_output(data_collection: DataCollection, bands: Optional[List[str]], meta_bands: List[str]) -> None:
    merged_output = "merged_bands"
    evalscript = generate_evalscript(
        data_collection=data_collection, bands=bands, meta_bands=meta_bands, merged_output=merged_output
    )
    *_, output_spec = parse_evalscript_info(evalscript)

    num_bands = len(data_collection.bands) if bands is None else len(bands)
    depth = [depth for name, depth, _ in output_spec if name == merged_output][0]

    assert len(output_spec) == 1 + len(meta_bands)
    assert int(depth) == num_bands


@pytest.mark.parametrize("merged_output", [None, "merged_bands"])
@pytest.mark.parametrize("use_dn", [True, False])
def test_sample_type(merged_output: Optional[str], use_dn: bool) -> None:
    data_collection = DataCollection.SENTINEL2_L1C
    evalscript = generate_evalscript(data_collection=data_collection, merged_output=merged_output, use_dn=use_dn)

    *_, output_spec = parse_evalscript_info(evalscript)
    assert len(output_spec) == 1 if merged_output else len(data_collection.bands)

    sample_types_set = {sample_type for *_, sample_type in output_spec}
    assert sample_types_set == {"UINT16" if use_dn else "FLOAT32"}
