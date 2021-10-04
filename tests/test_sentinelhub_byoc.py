"""
A module that tests an interface for Sentinel Hub Batch processing
"""
import os
from datetime import datetime

import dateutil.tz
import pytest

from sentinelhub import SentinelHubBYOC, ByocCollection, ByocTile, SHConfig, DownloadFailedException

pytestmark = pytest.mark.sh_integration


@pytest.fixture(name='config')
def config_fixture():
    config = SHConfig()
    for param in config.get_params():
        env_variable = param.upper()
        if os.environ.get(env_variable):
            setattr(config, param, os.environ.get(env_variable))
    return config


@pytest.fixture(name='byoc')
def byoc_fixture(config):
    return SentinelHubBYOC(config=config)


@pytest.fixture(name='collection')
def collection_fixture():
    return {'id': '7453e962-0ee5-4f74-8227-89759fbe9ba9',
            'userId': '1b639ce6-eb3e-494c-9cb4-2eab3569b121',
            'name': 'SI LULC Reference',
            's3Bucket': 'eo-learn.sentinel-hub.com',
            'additionalData': {'bands': {'lulc_reference': {'bitDepth': 8}},
                               'maxMetersPerPixel': 800.0},
            'created': '2020-06-22T12:30:22.814Z'}


@pytest.fixture(name='tile')
def tile_fixture():
    return {'id': '8ac6e49c-996b-49cc-a0d4-ff67491b7a97',
            'path': 'maps/si_(BAND).tiff',
            'status': 'INGESTED',
            'tileGeometry': {'crs': {'type': 'name',
                                     'properties': {'name': 'urn:ogc:def:crs:EPSG::32633'}},
                             'type': 'Polygon',
                             'coordinates': (((370000.0, 5195000.0),
                                              (620000.0, 5195000.0),
                                              (620000.0, 5025000.0),
                                              (370000.0, 5025000.0),
                                              (370000.0, 5195000.0)),)},
            'coverGeometry': {'crs': {'type': 'name',
                                      'properties': {'name': 'urn:ogc:def:crs:EPSG::32633'}},
                              'type': 'MultiPolygon',
                              'coordinates': [(((369999.99998228427, 5025000.000464492),
                                                (620000.000010147, 5025000.000464773),
                                                (620000.000012391, 5195000.000585059),
                                                (369999.9999783558, 5195000.00058473),
                                                (369999.99998228427, 5025000.000464492)),)]},
            'created': '2020-06-22T12:33:36.081000Z',
            'sensingTime': None,
            'additionalData': {'minMetersPerPixel': 10.0, 'maxMetersPerPixel': 160.0}}


def test_get_collections(byoc):
    collections = list(byoc.iter_collections())

    assert len(collections) >= 0
    assert all(isinstance(collection, dict) for collection in collections)


def test_get_collection(byoc, collection):
    sh_collection = byoc.get_collection(collection)

    assert isinstance(sh_collection, dict)
    assert ByocCollection.from_dict(sh_collection) == ByocCollection.from_dict(collection)


def test_get_non_existing_collection(byoc):
    with pytest.raises(DownloadFailedException):
        byoc.get_collection(collection='sinergise')


def test_get_tiles(byoc, collection, tile):
    tiles = list(byoc.iter_tiles(collection))

    assert len(tiles) == 1
    assert all(isinstance(tile, dict) for tile in tiles)
    assert ByocTile.from_dict(tiles[0]) == ByocTile.from_dict(tile)


def test_get_tile(byoc, collection, tile):
    sh_tile = byoc.get_tile(collection=collection, tile=tile)

    assert isinstance(sh_tile, dict)
    assert ByocTile.from_dict(sh_tile) == ByocTile.from_dict(tile)


def test_create_collection(byoc, requests_mock):
    requests_mock.post('/oauth/token', real_http=True)
    mocked_url = '/api/v1/byoc/collections'

    new_collection = ByocCollection(name='mocked collection', s3_bucket='mocked_bucket')
    requests_mock.post(mocked_url, json={'data': new_collection.to_dict()})

    response = byoc.create_collection(collection=new_collection)

    assert ByocCollection.from_dict(response) == new_collection


def test_update_collection(byoc, collection, requests_mock):
    requests_mock.post('/oauth/token', real_http=True)
    mocked_url = f'/api/v1/byoc/collections/{collection["id"]}'

    updated_collection = dict(collection)
    updated_collection['name'] = 'updated collection'
    requests_mock.put(mocked_url, content=None)

    response = byoc.update_collection(collection=updated_collection)

    assert response == ''


def test_delete_collection(byoc, collection, requests_mock):
    requests_mock.post('/oauth/token', real_http=True)
    mocked_url = f'/api/v1/byoc/collections/{collection["id"]}'

    requests_mock.delete(mocked_url, content=None)

    response = byoc.delete_collection(collection=collection)

    assert response == ''


def test_create_tile(byoc, collection, requests_mock):
    requests_mock.post('/oauth/token', real_http=True)
    mocked_url = f'/api/v1/byoc/collections/{collection["id"]}/tiles'

    tile = ByocTile(path='mocked/path/mocked.tiff', sensing_time=datetime.now(tz=dateutil.tz.tzutc()))
    requests_mock.post(mocked_url, json={'data': tile.to_dict()})

    response = byoc.create_tile(collection=collection, tile=tile)

    assert ByocTile.from_dict(response) == tile


def test_update_tile(byoc, collection, tile, requests_mock):
    requests_mock.post('/oauth/token', real_http=True)
    mocked_url = f'/api/v1/byoc/collections/{collection["id"]}/tiles/{tile["id"]}'

    updated_tile = dict(tile)
    updated_tile['sensingTime'] = datetime.now().isoformat()

    requests_mock.put(mocked_url, content=None)

    response = byoc.update_tile(collection=collection, tile=updated_tile)

    assert response == ''


def test_delete_tile(byoc, collection, tile, requests_mock):
    requests_mock.post('/oauth/token', real_http=True)
    mocked_url = f'/api/v1/byoc/collections/{collection["id"]}/tiles/{tile["id"]}'

    requests_mock.delete(mocked_url, content=None)

    response = byoc.delete_tile(collection=collection, tile=tile)

    assert response == ''
