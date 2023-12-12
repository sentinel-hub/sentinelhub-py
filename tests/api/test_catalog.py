"""
Tests for the module with Catalog API interface
"""

from __future__ import annotations

import datetime as dt
from functools import partial

import dateutil.tz
import numpy as np
import pytest

from sentinelhub import CRS, BBox, DataCollection, Geometry, SentinelHubCatalog, SHConfig, parse_time
from sentinelhub.api.catalog import CatalogSearchIterator, get_available_timestamps
from sentinelhub.constants import ServiceUrl

TEST_BBOX = BBox((46.16, -16.15, 46.51, -15.58), CRS.WGS84)
CDSE_UNSUPPORTED_COLLECTIONS = [DataCollection.LANDSAT_OT_L1, DataCollection.LANDSAT_OT_L2, DataCollection.MODIS]

pytestmark = pytest.mark.sh_integration


@pytest.fixture(name="sh_catalog")
def sh_catalog_fixture(request) -> SentinelHubCatalog:
    return SentinelHubCatalog(config=request.getfixturevalue("sh_config"))


@pytest.fixture(name="cdse_catalog")
def cdse_catalog_fixture(request) -> SentinelHubCatalog:
    return SentinelHubCatalog(config=request.getfixturevalue("cdse_config"))


@pytest.mark.parametrize(
    ("config", "data_collection", "doc_href"),
    [
        ("sh_config", DataCollection.SENTINEL2_L2A, "https://docs.sentinel-hub.com"),
        ("sh_config", DataCollection.LANDSAT_TM_L1, "https://docs.sentinel-hub.com"),
        ("sh_config", DataCollection.SENTINEL3_OLCI, "https://docs.sentinel-hub.com"),
        ("cdse_config", DataCollection.SENTINEL2_L2A, "https://documentation.dataspace.copernicus.eu"),
    ],
)
def test_info_with_different_deployments(
    config: SHConfig, data_collection: list[DataCollection], doc_href: str, request
) -> None:
    """Test if basic interaction works with different data collections on different deployments"""
    config = request.getfixturevalue(config)
    if config.sh_base_url != ServiceUrl.CDSE:
        config.sh_base_url = data_collection.service_url or config.sh_base_url
    catalog = SentinelHubCatalog(config=config)
    info = catalog.get_info()

    assert isinstance(info, dict)
    for link in info["links"]:
        assert link["href"].startswith(config.sh_base_url) or link["href"].startswith(doc_href)


@pytest.mark.parametrize("catalog", ["sh_catalog", "cdse_catalog"])
def test_conformance(catalog: SentinelHubCatalog, request) -> None:
    """Test conformance endpoint"""
    conformance = request.getfixturevalue(catalog).get_conformance()
    assert isinstance(conformance, dict)


@pytest.mark.parametrize("catalog", ["sh_catalog", "cdse_catalog"])
def test_get_collections(catalog: SentinelHubCatalog, request) -> None:
    """Tests collections endpoint"""
    collections = request.getfixturevalue(catalog).get_collections()

    assert isinstance(collections, list)
    assert len(collections) >= 3


@pytest.mark.parametrize("collection_input", ["sentinel-2-l1c", DataCollection.SENTINEL1_IW])
@pytest.mark.parametrize("catalog", ["sh_catalog", "cdse_catalog"])
def test_get_collection(catalog: SentinelHubCatalog, collection_input: DataCollection | str, request) -> None:
    """Test endpoint for a single collection info"""
    collection_info = request.getfixturevalue(catalog).get_collection(collection_input)
    assert isinstance(collection_info, dict)


@pytest.mark.parametrize(
    ("catalog", "feature_id"),
    [
        ("sh_catalog", "S2A_MSIL2A_20231206T100401_N0509_R122_T33TTG_20231206T123051"),
        ("cdse_catalog", "S2A_MSIL2A_20231206T100401_N0509_R122_T33TTG_20231206T123051.SAFE"),
    ],
)
def test_get_feature(catalog: SentinelHubCatalog, feature_id: str, request) -> None:
    """Test endpoint for a single feature info"""
    feature_info = request.getfixturevalue(catalog).get_feature(DataCollection.SENTINEL2_L2A, feature_id)

    assert isinstance(feature_info, dict)
    assert feature_info["id"] == feature_id


@pytest.mark.parametrize("catalog", ["sh_catalog", "cdse_catalog"])
def test_search_bbox(catalog: SentinelHubCatalog, request) -> None:
    """Tests search with bounding box"""
    time_interval = "2021-01-01T00:00:00", "2021-01-15T00:00:10"

    search_iterator = request.getfixturevalue(catalog).search(
        collection=DataCollection.SENTINEL2_L1C,
        time=time_interval,
        bbox=TEST_BBOX.transform(CRS.POP_WEB),
        limit=2,
    )

    assert isinstance(search_iterator, CatalogSearchIterator)

    for result in search_iterator:
        assert isinstance(result, dict)
        assert time_interval[0] <= result["properties"]["datetime"] <= time_interval[1]


@pytest.mark.parametrize(
    ("catalog", "exp_unbounded_len", "exp_filtered_len"), [("sh_catalog", 6, 4), ("cdse_catalog", 6, 2)]
)
def test_search_filter(catalog: SentinelHubCatalog, exp_unbounded_len: int, exp_filtered_len: int, request) -> None:
    time_interval = "2021-01-01T00:00:00", "2021-01-31T00:00:10"
    min_cc, max_cc = 10, 20
    common_kwargs = dict(
        collection=DataCollection.SENTINEL2_L1C,
        time=time_interval,
        bbox=TEST_BBOX.transform(CRS.POP_WEB),
        limit=2,
    )
    catalog = request.getfixturevalue(catalog)
    unbounded_search_iterator = catalog.search(**common_kwargs)
    assert len(list(unbounded_search_iterator)) == exp_unbounded_len

    text_filter_iterator = catalog.search(
        filter=f"eo:cloud_cover>{min_cc} AND eo:cloud_cover<{max_cc}",
        **common_kwargs,
    )
    text_filtered = list(text_filter_iterator)

    assert len(text_filtered) == exp_filtered_len
    assert all(min_cc < result["properties"]["eo:cloud_cover"] < max_cc for result in text_filtered)

    json_filter_iterator = catalog.search(
        filter={
            "op": "and",
            "args": [
                {"op": ">", "args": [{"property": "eo:cloud_cover"}, min_cc]},
                {"op": "<", "args": [{"property": "eo:cloud_cover"}, max_cc]},
            ],
        },
        filter_lang="cql2-json",
        **common_kwargs,
    )

    assert text_filtered == list(json_filter_iterator)


@pytest.mark.parametrize(
    ("catalog", "feature_id", "timestamp"),
    [
        (
            "sh_catalog",
            "S2A_MSIL1C_20210103T071211_N0209_R020_T38LPH_20210103T083459",
            dt.datetime(2021, 1, 3, 7, 14, 7, tzinfo=dateutil.tz.tzutc()),
        ),
        (
            "cdse_catalog",
            "S2A_MSIL1C_20210103T071211_N0500_R020_T38LPH_20230228T175137.SAFE",
            dt.datetime(2021, 1, 3, 7, 14, 7, 540000, tzinfo=dateutil.tz.tzutc()),
        ),
    ],
)
def test_search_geometry_and_iterator_methods(
    catalog: SentinelHubCatalog, feature_id: str, timestamp: dt.datetime, request
) -> None:
    """Tests search with a geometry and test methods of CatalogSearchIterator"""
    search_geometry = Geometry(TEST_BBOX.geometry, crs=TEST_BBOX.crs)

    search_iterator = request.getfixturevalue(catalog).search(
        collection=DataCollection.SENTINEL2_L1C,
        time=("2021-01-01", "2021-01-5"),
        geometry=search_geometry,
        filter="eo:cloud_cover<40",
    )
    results = list(search_iterator)

    assert len(results) == 1
    assert search_iterator.get_timestamps() == [timestamp]
    assert search_iterator.get_ids() == [feature_id]

    geometries = search_iterator.get_geometries()
    assert len(geometries) == 1
    assert isinstance(geometries[0], Geometry)
    assert geometries[0].geometry.intersects(search_geometry.geometry)


@pytest.mark.parametrize(
    ("data_collection", "feature_id"),
    [
        (
            DataCollection.SENTINEL2_L1C,
            dict(
                sh="S2A_MSIL1C_20210113T071211_N0209_R020_T38LPH_20210113T075941",
                cdse="S2A_MSIL1C_20210113T071211_N0500_R020_T38LPH_20230520T010221.SAFE",
            ),
        ),
        (
            "sentinel-2-l1c",
            dict(
                sh="S2A_MSIL1C_20210113T071211_N0209_R020_T38LPH_20210113T075941",
                cdse="S2A_MSIL1C_20210113T071211_N0500_R020_T38LPH_20230520T010221.SAFE",
            ),
        ),
        (
            DataCollection.SENTINEL2_L2A,
            dict(
                sh="S2A_MSIL2A_20210113T071211_N0214_R020_T38LPH_20210113T083244",
                cdse="S2A_MSIL2A_20210113T071211_N0500_R020_T38LPH_20230520T064324.SAFE",
            ),
        ),
        (
            DataCollection.SENTINEL1_IW,
            dict(
                sh="S1A_IW_GRDH_1SDV_20210113T022710_20210113T022735_036113_043BC9_2981",
                cdse="S1A_IW_GRDH_1SDV_20210113T022710_20210113T022735_036113_043BC9_E7C4_COG.SAFE",
            ),
        ),
        (DataCollection.LANDSAT_OT_L1, dict(sh="LC08_L1TP_160071_20210113_20210308_02_T1")),
        (DataCollection.LANDSAT_OT_L2, dict(sh="LC08_L2SP_160071_20210113_20210308_02_T1")),
        (DataCollection.MODIS, dict(sh="MCD43A4.006/22/10/2021014/MCD43A4.A2021014.h22v10.006.2021025214119")),
        (
            DataCollection.SENTINEL3_OLCI,
            dict(
                sh="S3A_OL_1_EFR____20210114T063914_20210114T064214_20210115T095516_0179_067_191_3240_LN1_O_NT_002.SEN3",
                cdse="S3A_OL_1_EFR____20210114T063914_20210114T064214_20210115T095516_0179_067_191_3240_LN1_O_NT_002.SEN3",
            ),
        ),
        (
            DataCollection.SENTINEL3_SLSTR,
            dict(
                sh="S3A_SL_1_RBT____20210114T190809_20210114T191109_20210116T001252_0179_067_198_5760_LN2_O_NT_004.SEN3",
                cdse="S3A_SL_1_RBT____20210114T190809_20210114T191109_20210116T001252_0179_067_198_5760_LN2_O_NT_004.SEN3",
            ),
        ),
        (
            DataCollection.SENTINEL5P,
            dict(
                sh="S5P_NRTI_L2__AER_AI_20210114T100354_20210114T100854_16869_01_010400_20210114T104450",
                cdse="S5P_NRTI_L2__AER_AI_20210114T100354_20210114T100854_16869_01_010400_20210114T104450.nc",
            ),
        ),
    ],
)
@pytest.mark.parametrize("config", ["sh_config", "cdse_config"])
def test_search_for_data_collection(
    config: SHConfig, data_collection: DataCollection | str, feature_id: dict[str, str], request
) -> None:
    """Tests search functionality for each data collection to confirm compatibility between DataCollection parameters
    and Catalog API
    """
    if config == "cdse_config" and data_collection in CDSE_UNSUPPORTED_COLLECTIONS:
        pytest.skip("Unsupported collections on CDSE")
    collection_base_url = data_collection.service_url if isinstance(data_collection, DataCollection) else None
    endpoint = config.split("_")[0]
    config = request.getfixturevalue(config)
    if endpoint != "cdse":
        config.sh_base_url = collection_base_url or config.sh_base_url
    catalog = SentinelHubCatalog(config=config)

    search_iterator = catalog.search(
        collection=data_collection,
        time=("2021-01-01T00:00:00", "2021-01-15T00:00:10"),
        bbox=TEST_BBOX,
        limit=1,
    )
    result = next(search_iterator)
    assert isinstance(result, dict)
    assert result["id"] == feature_id[endpoint]


@pytest.mark.parametrize(
    ("config", "data_collection", "tile_id"),
    [
        ("sh_config", DataCollection.LANDSAT_ETM_L1, "LE07_L1TP_160071_20170110_20201008_02_T1"),
        (
            "cdse_config",
            DataCollection.SENTINEL5P,
            "S5P_NRTI_L2__AER_AI_20210114T100354_20210114T100854_16869_01_010400_20210114T104450.nc",
        ),
    ],
)
def test_search_with_ids(config: SHConfig, data_collection: DataCollection, tile_id: str, request) -> None:
    """Tests a search without time and bbox parameters"""
    config = request.getfixturevalue(config)
    if config.sh_base_url != ServiceUrl.CDSE:
        config.sh_base_url = data_collection.service_url
    catalog = SentinelHubCatalog(config=config)

    search_iterator = catalog.search(collection=data_collection, ids=[tile_id])
    results = list(search_iterator)
    assert len(results) == 1
    assert results[0]["id"] == tile_id


@pytest.mark.parametrize(
    ("data_collection", "time_difference_hours", "maxcc", "n_timestamps"),
    [
        (DataCollection.SENTINEL1_IW, 2, None, 4),
        (DataCollection.SENTINEL2_L2A, 1, 0.7, 8),
        (DataCollection.SENTINEL2_L2A, 2 * 30 * 24, None, 1),
        (DataCollection.SENTINEL2_L1C.define_from("COLLECTION_WITHOUT_URL", service_url=None), -1, None, 10),
    ],
)
@pytest.mark.parametrize("config", ["sh_config", "cdse_config"])
def test_get_available_timestamps(
    data_collection: DataCollection,
    time_difference_hours: int,
    maxcc: int,
    n_timestamps: int,
    config: SHConfig,
    request,
) -> None:
    interval_start, interval_end = "2019-04-20", "2019-06-09"
    get_test_timestamps = partial(
        get_available_timestamps,
        bbox=TEST_BBOX,
        data_collection=data_collection,
        time_difference=dt.timedelta(hours=time_difference_hours),
        time_interval=(interval_start, interval_end),
        maxcc=maxcc,
        config=request.getfixturevalue(config),
    )

    timestamps = get_test_timestamps(ignore_tz=True)
    assert len(timestamps) == n_timestamps
    assert all(ts >= parse_time(interval_start, force_datetime=True) for ts in timestamps)
    assert all(ts <= parse_time(interval_end, force_datetime=True) for ts in timestamps)
    assert all(ts_diff.total_seconds() / 3600 > time_difference_hours for ts_diff in np.diff(np.array(timestamps)))

    timestamps_with_tz = get_test_timestamps(ignore_tz=False)
    assert all(timestamp.tzinfo is not None for timestamp in timestamps_with_tz)
