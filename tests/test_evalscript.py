from typing import List, Optional

import pytest

from sentinelhub import CRS, BBox, DataCollection, MimeType, SentinelHubRequest, generate_evalscript


@pytest.mark.parametrize("data_collection", [DataCollection.SENTINEL2_L1C, DataCollection.SENTINEL1_IW])
def test_collection_bands(data_collection: DataCollection) -> None:
    evalscript = generate_evalscript(data_collection=data_collection)
    expected_bands_str = "bands: [" + ", ".join(f'"{b.name}"' for b in data_collection.bands) + "]"
    assert expected_bands_str in evalscript


@pytest.mark.parametrize(
    "data_collection, bands",
    [
        (DataCollection.SENTINEL2_L2A, ["B04", "B03", "B02"]),
        (DataCollection.SENTINEL1_IW, ["VV", "VH"]),
    ],
    ids=str,
)
def test_explicit_bands(data_collection: DataCollection, bands: List[str]) -> None:
    evalscript = generate_evalscript(data_collection=data_collection, bands=bands)
    expected_bands_str = "bands: [" + ", ".join(f'"{b}"' for b in bands) + "]"
    assert expected_bands_str in evalscript


@pytest.mark.parametrize(
    "meta_bands",
    [["CLM"], ["CLP", "dataMask"]],
    ids=str,
)
def test_explicit_meta_bands(meta_bands: List[str]) -> None:
    data_collection = DataCollection.SENTINEL2_L1C
    evalscript = generate_evalscript(data_collection=data_collection, meta_bands=meta_bands)

    all_bands = [f"{b.name}" for b in data_collection.bands] + meta_bands
    expected_bands_str = "bands: [" + ", ".join(f'"{b}"' for b in all_bands) + "]"
    assert expected_bands_str in evalscript


@pytest.mark.parametrize(
    "data_collection, bands, meta_bands",
    [
        (DataCollection.LANDSAT_TM_L2, ["B05", "B04", "B03", "B02"], ["BQA"]),
        (DataCollection.SENTINEL2_L2A, ["B04", "B03", "B02"], ["CLP", "SCL"]),
    ],
    ids=str,
)
def test_merged_output(data_collection: DataCollection, bands: List[str], meta_bands: List[str]) -> None:
    merged_output = "merged_bands"
    evalscript = generate_evalscript(
        data_collection=data_collection, bands=bands, meta_bands=meta_bands, merged_output=merged_output
    )

    expected_bands_output_spec = "{ " + f'id: "{merged_output}", bands: {len(bands)}'
    assert expected_bands_output_spec in evalscript

    expected_bands_return_spec = f"{merged_output}: [" + ", ".join(f"sample.{b}" for b in bands) + "]"
    assert expected_bands_return_spec in evalscript


def test_units() -> None:
    bands = ["B04", "B03", "B02", "B02"]
    data_collection = DataCollection.SENTINEL2_L1C

    evalscript_dn = generate_evalscript(data_collection=data_collection, bands=bands)
    assert evalscript_dn.count('"DN"') == len(bands)
    assert evalscript_dn.count('"REFLECTANCE"') == 0

    evalscript_reflectance = generate_evalscript(data_collection=data_collection, bands=bands, prioritize_dn=False)
    assert evalscript_reflectance.count('"REFLECTANCE"') == len(bands)
    assert evalscript_reflectance.count('"DN"') == 0


@pytest.mark.parametrize("use_dn", [True, False], ids=lambda x: f"use_dn: {x}")
def test_sample_type(use_dn: bool) -> None:
    data_collection = DataCollection.SENTINEL2_L1C
    evalscript = generate_evalscript(data_collection=data_collection, prioritize_dn=use_dn)

    expected_uint_count = len(data_collection.bands) if use_dn else 0
    assert evalscript.count('"UINT16"') == expected_uint_count

    expected_float_count = len(data_collection.bands) if not use_dn else 0
    assert evalscript.count('"FLOAT32"') == expected_float_count


@pytest.mark.parametrize("use_dn", [True, False], ids=lambda x: f"use_dn: {x}")
def test_sample_type_merged(use_dn: bool) -> None:
    evalscript = generate_evalscript(
        data_collection=DataCollection.SENTINEL2_L1C, merged_output="bands", prioritize_dn=use_dn
    )

    expected_uint_count = 1 if use_dn else 0
    assert evalscript.count('"UINT16"') == expected_uint_count

    expected_float_count = 1 if not use_dn else 0
    assert evalscript.count('"FLOAT32"') == expected_float_count


@pytest.mark.sh_integration
@pytest.mark.parametrize("data_collection", [DataCollection.LANDSAT_TM_L2, DataCollection.SENTINEL2_L2A])
@pytest.mark.parametrize("merged_output", [None, "bands"])
@pytest.mark.parametrize("use_dn", [True, False])
def test_valid_evalscript(data_collection: DataCollection, merged_output: Optional[str], use_dn: bool) -> None:
    bands = ["B05", "B04", "B03"]
    meta_bands = ["dataMask"]
    evalscript = generate_evalscript(
        data_collection=data_collection,
        bands=bands,
        meta_bands=meta_bands,
        merged_output=merged_output,
        prioritize_dn=use_dn,
    )
    if merged_output is not None:
        responses = [SentinelHubRequest.output_response(merged_output, MimeType.TIFF)]
        responses.extend([SentinelHubRequest.output_response(band, MimeType.TIFF) for band in meta_bands])
    else:
        responses = [SentinelHubRequest.output_response(band, MimeType.TIFF) for band in bands + meta_bands]

    request = SentinelHubRequest(
        evalscript=evalscript,
        input_data=[
            SentinelHubRequest.input_data(
                data_collection=data_collection,
                time_interval=("2017-12-15T07:12:03", "2017-12-15T07:12:04"),
                maxcc=0.8,
            )
        ],
        responses=responses,
        bbox=BBox(bbox=(14.51, 46.05, 14.51, 46.05), crs=CRS.WGS84),
        size=(10, 20),
    )

    # test passes if request doesn't fail
    request.get_data(max_threads=3)
