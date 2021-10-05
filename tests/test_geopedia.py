import datetime
from dataclasses import dataclass
from typing import Optional

import pytest
import numpy as np

from sentinelhub import (
    GeopediaSession, GeopediaWmsRequest, GeopediaImageRequest, GeopediaFeatureIterator, CRS, MimeType, BBox
)
from sentinelhub.testing_utils import test_numpy_data

pytestmark = pytest.mark.geopedia_integration


# When config.json could store Geopedia credentials add some login tests

def test_global_session():
    session1 = GeopediaSession(is_global=True)
    session2 = GeopediaSession(is_global=True)
    session3 = GeopediaSession(is_global=False)

    assert session1.session_id == session2.session_id, 'Global sessions should have the same session ID'
    assert session1.session_id != session3.session_id, 'Global and local sessions should not have the same session ID'


def test_session_update():
    session = GeopediaSession()
    initial_session_id = session.session_id

    assert session.restart().session_id == initial_session_id, 'Session should be updated'
    assert session.user_id == 'NO_USER', "Session user ID should be 'NO_USER'"


def test_session_timeout():
    session = GeopediaSession()
    session.SESSION_DURATION = datetime.timedelta(seconds=-1)
    initial_session_id = session.session_id

    assert session.session_id == initial_session_id, 'Session should timeout and be updated'


@pytest.mark.parametrize('bad_kwargs', [
    dict(username='some_user'),
    dict(password='some_password'),
    dict(username='some_user', password='some_password', password_md5='md5_encoded'),
])
def test_false_initialization(bad_kwargs):
    with pytest.raises(ValueError):
        GeopediaSession(**bad_kwargs)


@pytest.mark.xfail(run=True, reason='Geopedia sometimes returns numerically wrong data')
def test_geopedia_wms():
    bbox = BBox(bbox=[(524358.0140363087, 6964349.630376049), (534141.9536568124, 6974133.5699965535)], crs=CRS.POP_WEB)
    gpd_request = GeopediaWmsRequest(layer=1917, theme='ml_aws', bbox=bbox, width=50, height=50,
                                     image_format=MimeType.PNG)
    data = gpd_request.get_data()

    assert isinstance(data, list)
    assert len(data) == 1

    test_numpy_data(np.array(data), exp_min=0, exp_max=255, exp_mean=150.9248, exp_median=255)


def test_geopedia_image_request(output_folder):
    bbox = BBox(bbox=[(13520759, 437326), (13522689, 438602)], crs=CRS.POP_WEB)
    image_field_name = 'Masks'

    gpd_request = GeopediaImageRequest(
        layer=1749, bbox=bbox, image_field_name=image_field_name, image_format=MimeType.PNG, data_folder=output_folder,
        gpd_session=GeopediaSession(is_global=True)
    )
    image_list = gpd_request.get_data(save_data=True)

    assert isinstance(image_list, list)
    assert len(image_list) == 5

    test_numpy_data(np.array(image_list), exp_min=0, exp_max=255, exp_mean=66.88769, exp_median=0)

    filenames = gpd_request.get_filename_list()
    image_stats = list(gpd_request.get_items())[0]['properties'][image_field_name]

    for filename, image_stat in zip(filenames, image_stats):
        assert filename == image_stat['niceName']


@dataclass
class GeopediaFeatureIteratorTestCase:
    name: str
    params: dict
    min_features: int
    min_size: Optional[int] = None


BBOX = BBox(bbox=[(2947363, 4629723), (3007595, 4669471)], crs=CRS.POP_WEB).transform(CRS.WGS84)
TEST_CASES = [
    GeopediaFeatureIteratorTestCase(
        'All features', dict(layer=1749, gpd_session=GeopediaSession()), min_features=100, min_size=1609
    ),
    GeopediaFeatureIteratorTestCase(
        'BBox filter', dict(layer='1749', bbox=BBOX), min_features=21
    ),
    GeopediaFeatureIteratorTestCase(
        'Query Filter', dict(layer='ttl1749', query_filter='f12458==32632'), min_features=76
    ),
    GeopediaFeatureIteratorTestCase(
        'Both filters - No data', dict(layer=1749, bbox=BBOX, query_filter='f12458==32632'), min_features=0
    ),
    GeopediaFeatureIteratorTestCase(
        'Both filters - Some data', dict(layer=1749, bbox=BBOX, query_filter='f12458==32635'), min_features=21
    ),
]


@pytest.mark.parametrize('test_case', TEST_CASES)
def test_iterator(test_case):
    gpd_iter = GeopediaFeatureIterator(**test_case.params)

    for idx, feature in enumerate(gpd_iter):
        assert isinstance(feature, dict)
        if idx >= test_case.min_features - 1:
            break

    assert gpd_iter.index == test_case.min_features

    if test_case.min_size:
        assert test_case.min_size <= len(gpd_iter)


@pytest.mark.parametrize('test_case', TEST_CASES)
def test_size_before_iteration(test_case):
    if not test_case.min_features:
        return

    gpd_iter1 = GeopediaFeatureIterator(**test_case.params)
    _ = gpd_iter1.get_size()
    first_feature1 = next(gpd_iter1)

    gpd_iter2 = GeopediaFeatureIterator(**test_case.params)
    first_feature2 = next(gpd_iter2)

    assert first_feature1 == first_feature2
