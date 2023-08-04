"""
Unit tests for data_collections module
"""
from __future__ import annotations

from typing import Any

import pytest

from sentinelhub import DataCollection
from sentinelhub.data_collections import DataCollectionDefinition


@pytest.mark.parametrize(
    ("data_colection_def", "derive_attributes", "expected_attributes"),
    [
        (DataCollectionDefinition(), {}, {"api_id": None}),
        (
            DataCollectionDefinition(api_id="X", wfs_id="Y"),
            {"wfs_id": "Z"},
            {"api_id": "X", "wfs_id": "Z", "collection_type": None},
        ),
        (DataCollection.LANDSAT_MSS_L1.value, {"api_id": None}, {"api_id": None, "wfs_id": "DSS14"}),
    ],
)
def test_derive(
    data_colection_def: DataCollectionDefinition, derive_attributes: dict[str, Any], expected_attributes: dict[str, Any]
) -> None:
    derived_definition = data_colection_def.derive(**derive_attributes)

    for attribute, value in expected_attributes.items():
        assert value == getattr(derived_definition, attribute)


@pytest.mark.parametrize(
    ("definition_input", "expected"),
    [
        ({}, "DataCollectionDefinition(\n  is_timeless: False\n  has_cloud_coverage: False\n)"),
        (
            {"api_id": "X", "_name": "A"},
            "DataCollectionDefinition(\n  api_id: X\n  is_timeless: False\n  has_cloud_coverage: False\n)",
        ),
        (
            {"api_id": "Y", "is_timeless": True, "has_cloud_coverage": True},
            "DataCollectionDefinition(\n  api_id: Y\n  is_timeless: True\n  has_cloud_coverage: True\n)",
        ),
    ],
)
def test_collection_repr(definition_input: dict[str, Any], expected: str) -> None:
    assert repr(DataCollectionDefinition(**definition_input)) == expected


@pytest.mark.parametrize(
    ("test_definition", "equal_definition"),
    [
        ({"api_id": "X", "_name": "A"}, {"api_id": "X", "_name": "A"}),
        ({"api_id": "X", "_name": "A"}, {"api_id": "X", "_name": "B"}),
        ({"api_id": "X", "is_timeless": False}, {"api_id": "X", "is_timeless": False, "_name": "B"}),
        ({"api_id": "X", "is_timeless": False}, {"api_id": "X"}),
    ],
)
def test_collection_definitions_equal(test_definition: dict[str, Any], equal_definition: dict[str, Any]) -> None:
    def1 = DataCollectionDefinition(**test_definition)
    def2 = DataCollectionDefinition(**equal_definition)
    assert def1 == def2


@pytest.mark.parametrize(
    ("test_definition", "equal_definition"),
    [
        ({"api_id": "X", "_name": "A"}, {"api_id": "Y", "_name": "A"}),
        ({"api_id": "X", "is_timeless": True}, {"api_id": "X"}),
        ({"api_id": "X", "wfs_id": 2132342143454364}, {"api_id": "X"}),
    ],
)
def test_collection_definitions_not_equal(test_definition: dict[str, Any], equal_definition: dict[str, Any]) -> None:
    def1 = DataCollectionDefinition(**test_definition)
    def2 = DataCollectionDefinition(**equal_definition)
    assert def1 != def2


def test_define() -> None:
    data_collection = DataCollection.define("NEW", api_id="X", sensor_type="Sensor", bands=("B01",), is_timeless=True)

    assert data_collection == DataCollection.NEW
    assert DataCollection.NEW.api_id == "X"

    # Should fail because DataCollection with same api_id already exists.
    with pytest.raises(ValueError):
        DataCollection.define("NEW_NEW", api_id="X", sensor_type="Sensor", bands=("B01",), is_timeless=True)

    # Should fail because DataCollection with same name already exists.
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

    assert DataCollection.MY_BYOC.is_byoc
    assert not DataCollection.SENTINEL5P.is_byoc


def test_define_batch() -> None:
    batch_id = "0000d273-7e89-4f00-971e-9024f89a0000"
    batch = DataCollection.define_batch(batch_id, name="MY_BATCH")

    assert batch == DataCollection.MY_BATCH
    assert batch.api_id.endswith(batch_id)
    assert batch.collection_id == batch_id

    assert DataCollection.MY_BATCH.is_batch
    assert not DataCollection.SENTINEL2_L2A.is_batch


@pytest.mark.parametrize("data_collection", [DataCollection.SENTINEL3_OLCI, DataCollection.SENTINEL2_L2A])
@pytest.mark.parametrize("attribute", ["api_id", "catalog_id", "wfs_id", "service_url", "bands", "sensor_type"])
def test_attributes(data_collection: DataCollection, attribute: str) -> None:
    value = getattr(data_collection, attribute)
    assert value is not None
    assert value == getattr(data_collection.value, attribute)


def test_attributes_empty_fail() -> None:
    data_collection = DataCollection.define("EMPTY")

    for attr_name in ["api_id", "catalog_id", "wfs_id", "bands"]:
        with pytest.raises(ValueError):
            getattr(data_collection, attr_name)

    assert data_collection.service_url is None


@pytest.mark.parametrize(
    ("test_collection", "expected"),
    [
        (DataCollection.SENTINEL2_L1C, False),
        (DataCollection.SENTINEL1_EW, True),
        (DataCollection.LANDSAT_TM_L1, False),
    ],
)
def test_is_sentinel1(test_collection: DataCollection, expected: bool) -> None:
    assert test_collection.is_sentinel1 == expected


@pytest.mark.parametrize(
    ("collection", "direction", "expected"),
    [
        ("SENTINEL1_IW_ASC", "ascending", True),
        ("SENTINEL1_IW_ASC", "descending", False),
        ("SENTINEL1_IW_DES", "ascending", False),
        ("SENTINEL2_L2A", "descending", True),
        ("SENTINEL2_L2A", "ascending", True),
    ],
)
def test_contains_orbit_direction(collection: str, direction: str, expected: bool) -> None:
    data_collection = getattr(DataCollection, collection)
    assert data_collection.contains_orbit_direction(direction) == expected


def test_get_available_collections() -> None:
    number_of_collection = len(DataCollection.get_available_collections())
    DataCollection.define("NEW_NEW", api_id="Z")
    DataCollection.define_batch("batch_id", name="MY_NEW_BATCH")
    DataCollection.define_byoc("byoc_id", name="MY_NEW_BYOC")
    collections = DataCollection.get_available_collections()

    assert len(collections) == number_of_collection + 3
    assert all(isinstance(collection, DataCollection) for collection in collections)


def test_transfer_with_ray(ray: Any) -> None:
    """This test makes sure that the process of transferring a custom DataCollection object to a Ray worker and back
    works correctly.
    """
    collection = DataCollection.SENTINEL2_L1C.define_from("MY_NEW_COLLECTION", api_id="xxx")

    collection_future = ray.remote(lambda x: x).remote(collection)
    transferred_collection = ray.get(collection_future)

    assert collection is transferred_collection
