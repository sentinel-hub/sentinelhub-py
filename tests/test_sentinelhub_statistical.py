"""
Tests for the module with Statistical API
"""
import pytest

from sentinelhub import BBox, Geometry, CRS, DataCollection, SentinelHubStatistical


EVALSCRIPT = """
//VERSION=3
function setup() {
  return {
    input: [{
      bands: ["B04", "B08", "dataMask"],
      units: "DN"
    }],
    output: [
      {
        id: "bands",
        bands: ["B04", "B08"],
        sampleType: "UINT16"
      },
      {
        id: "index",
        bands: ["NDVI"],
        sampleType: "FLOAT32"
      },
      {
        id: "dataMask",
        bands: 1
      }]
  }
}

function evaluatePixel(samples) {
    let NDVI = index(samples.B08, samples.B04);
    return {
        bands: [samples.B04, samples.B08],
        index: [NDVI],
        dataMask: [samples.dataMask]
    };
}
"""

CALCULATIONS = {
    "default": {
        "statistics": {
            "default": {
                "percentiles": {
                    "k": [5, 50, 95]
                }
            }
        }
    }
}

BBOX = BBox((460750.0, 5092550.0, 461250.0, 5093050.0), crs=CRS.UTM_33N)
GEOMETRY = Geometry(BBOX.geometry, crs=BBOX.crs)


@pytest.mark.sh_integration
@pytest.mark.parametrize(
    argnames='evalscript, bbox, geometry, time_interval, resolution, aggregation_interval, data_collection, '
             'data_filters, calculations, results',
    ids=['bbox', 'geometry_no_calculations'],
    argvalues=[
        (
            EVALSCRIPT, BBOX, None, ('2021-04-01', '2021-04-10'), (10, 10), 'P1D', DataCollection.SENTINEL2_L1C,
            {'maxcc': 1}, CALCULATIONS, {'n_aggregations': 1, 'n_indices': 2, 'n_bands': {'index': 1, 'bands': 2}}
        ),
        (
            EVALSCRIPT, None, GEOMETRY, ('2021-04-01', '2021-04-10'), (10, 10), 'P1D', DataCollection.SENTINEL2_L2A,
            {}, None, {'n_aggregations': 1, 'n_indices': 2, 'n_bands': {'index': 1, 'bands': 2}}
        ),
    ])
def test_statistical_api(evalscript, bbox, geometry, time_interval, resolution, aggregation_interval,
                         data_collection, data_filters, calculations, results):
    aggregation = SentinelHubStatistical.aggregation(
        evalscript=evalscript,
        time_interval=time_interval,
        aggregation_interval=aggregation_interval,
        resolution=resolution
    )
    input_data = SentinelHubStatistical.input_data(data_collection, **data_filters)

    stats = SentinelHubStatistical(
        aggregation=aggregation,
        calculations=calculations,
        input_data=[input_data],
        bbox=bbox,
        geometry=geometry
    )
    res = stats.get_data()[0]

    assert res['status'] == 'OK', 'Wrong response status message'
    assert len(res['data']) == results['n_aggregations'], 'Wrong number of aggregations'
    assert len(res['data'][0]['outputs']) == results['n_indices'], 'Wrong number of indices'

    for output in res['data'][0]['outputs']:
        assert len(res['data'][0]['outputs'][output]['bands']) == results['n_bands'][output], \
            f'Wrong number of bands for {output}'
