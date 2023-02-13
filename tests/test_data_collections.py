"""
Unit tests for data_collections module
"""
from typing import Any

import pytest

from sentinelhub import DataCollection
from sentinelhub.data_collections import DataCollectionDefinition


def test_derive() -> None:
    definition = DataCollectionDefinition(api_id="X", wfs_id="Y")
    derived_definition = definition.derive(wfs_id="Z")

    assert derived_definition.api_id == "X"
    assert derived_definition.wfs_id == "Z"
    assert derived_definition.collection_type is None


def test_collection_string() -> None:
    dcd = DataCollectionDefinition(api_id="X", _name="A")
    assert str(dcd) == "DataCollectionDefinition(\n  api_id: X\n  is_timeless: False\n  has_cloud_coverage: False\n)"


def test_compare() -> None:
    def1 = DataCollectionDefinition(api_id="X", _name="A")
    def2 = DataCollectionDefinition(api_id="X", _name="B")

    assert def1 == def2


def test_define() -> None:
    data_collection = DataCollection.define("NEW", api_id="X", sensor_type="Sensor", bands=("B01",), is_timeless=True)

    assert data_collection == DataCollection.NEW

    with pytest.raises(ValueError):
        DataCollection.define("NEW_NEW", api_id="X", sensor_type="Sensor", bands=("B01",), is_timeless=True)

    with pytest.raises(ValueError):
        DataCollection.define("NEW", api_id="Y")


def test_define_from() -> None:
    bands = ["B01", "XYZ"]
    data_collection = DataCollection.define_from(DataCollection.SENTINEL5P, "NEW_5P", api_id="X", bands=bands)

    assert data_collection == DataCollection.NEW_5P
    assert data_collection.api_id == "X"
    assert data_collection.wfs_id == DataCollection.SENTINEL5P.wfs_id
    assert data_collection.bands == tuple(bands)


def test_define_byoc() -> None:
    byoc_id = "0000d273-7e89-4f00-971e-9024f89a0000"
    byoc = DataCollection.define_byoc(byoc_id, name="MY_BYOC")

    assert byoc == DataCollection.MY_BYOC
    assert byoc.api_id.endswith(byoc_id)
    assert byoc.collection_id == byoc_id


def test_define_batch() -> None:
    batch_id = "0000d273-7e89-4f00-971e-9024f89a0000"
    batch = DataCollection.define_batch(batch_id, name="MY_BATCH")

    assert batch == DataCollection.MY_BATCH
    assert batch.api_id.endswith(batch_id)
    assert batch.collection_id == batch_id


def test_attributes() -> None:
    data_collection = DataCollection.SENTINEL3_OLCI

    for attr_name in ["api_id", "catalog_id", "wfs_id", "service_url", "bands", "sensor_type"]:
        value = getattr(data_collection, attr_name)
        assert value is not None
        assert value == getattr(data_collection.value, attr_name)


def test_attributes_empty_fail() -> None:
    data_collection = DataCollection.define("EMPTY")

    for attr_name in ["api_id", "catalog_id", "wfs_id", "bands"]:
        with pytest.raises(ValueError):
            getattr(data_collection, attr_name)

    assert data_collection.service_url is None


@pytest.mark.parametrize(
    "collection, collection_type, expaected",
    [
        ("SENTINEL2_L1C", "is_sentinel1", False),
        ("SENTINEL2_L1C", "is_byoc", False),
        ("SENTINEL2_L1C", "is_batch", False),
        ("SENTINEL1_EW", "is_sentinel1", True),
        ("SENTINEL1_EW", "is_byoc", False),
        ("SENTINEL1_EW", "is_batch", False),
    ],
)
def test_sentinel_is_checks(collection: str, collection_type: str, expaected: bool) -> None:
    data_collection = getattr(DataCollection, collection)
    assert getattr(data_collection, collection_type) == expaected


@pytest.mark.parametrize(
    "collection, direction, expaected",
    [
        ("SENTINEL1_IW_ASC", "ascending", True),
        ("SENTINEL1_IW_ASC", "descending", False),
        ("SENTINEL1_IW_DES", "ascending", False),
        ("SENTINEL2_L2A", "descending", True),
        ("SENTINEL2_L2A", "ascending", True),
    ],
)
def test_contains_orbit_direction(collection: str, direction: str, expaected: bool) -> None:
    data_collection = getattr(DataCollection, collection)
    assert data_collection.contains_orbit_direction(direction) == expaected


def test_get_available_collections() -> None:
    collections = DataCollection.get_available_collections()
    assert len(collections) >= 28
    assert all([type(collection) == DataCollection for collection in collections])


def test_transfer_with_ray(ray: Any) -> None:
    """This test makes sure that the process of transferring a custom DataCollection object to a Ray worker and back
    works correctly.
    """
    collection = DataCollection.SENTINEL2_L1C.define_from("MY_NEW_COLLECTION", api_id="xxx")

    collection_future = ray.remote(lambda x: x).remote(collection)
    transferred_collection = ray.get(collection_future)

    assert collection is transferred_collection
