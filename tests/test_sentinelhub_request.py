""" Tests for the Processing API requests
"""
import unittest
import numpy as np

from shapely.geometry import Polygon
from sentinelhub import SentinelHubRequest, CRS, BBox, TestSentinelHub, DataSource, MimeType, bbox_to_dimensions, \
    Geometry


class TestSentinelHubRequest(TestSentinelHub):
    """ Tests for the Processing API requests
    """

    def test_single_tiff(self):
        """ Test downloading three bands of L1C
        """
        evalscript = """
            //VERSION=3

            function setup() {
                return {
                    input: [{
                        bands: ["B02", "B03", "B04"],
                        units: "REFLECTANCE"
                    }],
                    output: {
                        bands: 3,
                        sampleType: "FLOAT32"
                    }
                };
            }

            function evaluatePixel(sample) {
                return [2.5 * sample.B04, 2.5 * sample.B03, 2.5 * sample.B02];
            }
        """

        request = SentinelHubRequest(
            evalscript=evalscript,
            input_data=[
                SentinelHubRequest.input_data(
                    data_source=DataSource.SENTINEL2_L1C,
                    time_interval=('2017-12-15T07:12:03', '2017-12-15T07:12:04'),
                    maxcc=0.8
                )
            ],
            responses=[
                SentinelHubRequest.output_response('default', MimeType.TIFF)
            ],
            bbox=BBox(bbox=[46.16, -16.15, 46.51, -15.58], crs=CRS.WGS84),
            size=(512, 856),
            mime_type=MimeType.TIFF
        )

        img = request.get_data(max_threads=3)[0]

        self.assertEqual(img.shape, (856, 512, 3))
        self.test_numpy_data(img, exp_min=0, exp_max=2.01825, exp_mean=0.29408056)

    def test_other_args(self):
        """ Test downloading three bands of L1C
        """
        evalscript = """
            //VERSION=3

            function setup() {
                return {
                    input: ["B02", "B03", "B04"],
                    sampleType: "UINT16",
                    output: { bands: 3 }
                };
            }

            function evaluatePixel(sample) {
                return [2.5 * sample.B04, 2.5 * sample.B03, 2.5 * sample.B02];
            }
        """

        request = SentinelHubRequest(
            evalscript=evalscript,
            input_data=[
                SentinelHubRequest.input_data(
                    data_source=DataSource.SENTINEL2_L1C,
                    time_interval=('2017-12-15T07:12:03', '2017-12-15T07:12:04'),
                    maxcc=0.8,
                    other_args={
                        'processing': {
                            'upsampling': 'NEAREST',
                            'downsampling': 'NEAREST',
                            'atmosphericCorrection': 'NONE'
                        }
                    }
                )
            ],
            responses=[
                SentinelHubRequest.output_response('default', 'tiff')
            ],
            bbox=BBox(bbox=[46.16, -16.15, 46.51, -15.58], crs=CRS.WGS84),
            size=(512, 856),
            mime_type=MimeType.TIFF
        )

        img = request.get_data(max_threads=3)[0]

        self.assertEqual(img.shape, (856, 512, 3))
        self.test_numpy_data(img, exp_min=0, exp_max=255, exp_mean=74.92)

    def test_resolution_parameter(self):
        """ Test downloading three bands of L1C
        """
        evalscript = """
            //VERSION=3

            function setup() {
                return {
                    input: [{
                        bands: ["B02", "B03", "B04"],
                        units: "DN"
                    }],
                    output: {
                        bands: 3,
                        sampleType: "UINT16"
                    }
                };
            }

            function evaluatePixel(sample) {
                return [2.5 * sample.B04, 2.5 * sample.B03, 2.5 * sample.B02];
            }
        """

        request = SentinelHubRequest(
            evalscript=evalscript,
            input_data=[
                SentinelHubRequest.input_data(
                    data_source=DataSource.SENTINEL2_L1C,
                    time_interval=('2017-10-14T00:12:03', '2017-12-15T23:12:04'),
                    mosaicking_order='leastCC'
                )
            ],
            responses=[
                SentinelHubRequest.output_response('default', MimeType.TIFF)
            ],

            bbox=BBox(
                bbox=[1155360.393335921, 5285081.168940068, 1156965.063795706, 5286609.808304847],
                crs=CRS.POP_WEB
            ),
            resolution=(10.0, 10.0),
            mime_type=MimeType.TIFF
        )

        img = request.get_data(max_threads=3)[0]

        self.assertEqual(img.shape, (153, 160, 3))
        self.test_numpy_data(img, exp_min=670, exp_max=6105, exp_mean=1848.2646)

    def test_multipart_tar(self):
        """ Test downloading multiple outputs, packed into a TAR file
        """
        evalscript = """
            //VERSION=3

            function setup() {
                return {
                    input: [{
                        bands: ["B02", "B03", "B04"],
                        units: "DN"
                    }],
                    output: {
                        bands: 3,
                        sampleType: "UINT16"
                    }
                };
            }

            function updateOutputMetadata(scenes, inputMetadata, outputMetadata) {
                outputMetadata.userData = { "norm_factor":  inputMetadata.normalizationFactor }
            }

            function evaluatePixel(sample) {
                return [sample.B02, sample.B03, sample.B04];
            }
        """

        request = SentinelHubRequest(
            evalscript=evalscript,
            input_data=[
                SentinelHubRequest.input_data(
                    data_source=DataSource.SENTINEL2_L1C,
                    time_interval=('2017-12-15T07:12:03', '2017-12-15T07:12:04'),
                    maxcc=0.8
                )
            ],
            responses=[
                SentinelHubRequest.output_response('default', MimeType.TIFF),
                SentinelHubRequest.output_response('userdata', MimeType.JSON)
            ],
            bbox=BBox(bbox=[46.16, -16.15, 46.51, -15.58], crs=CRS.WGS84),
            size=(512, 856),
            mime_type=MimeType.TAR
        )

        tar = request.get_data(max_threads=3)[0]

        self.assertTrue('default.tif' in tar)
        self.assertTrue('userdata.json' in tar)

        img_data = tar['default.tif']

        self.assertEqual(img_data.shape, (856, 512, 3))
        self.test_numpy_data(img_data, exp_min=0, exp_max=8073, exp_mean=1176.32)

        json_data = tar['userdata.json']

        self.assertTrue('norm_factor' in json_data)
        self.assertTrue(json_data['norm_factor'] == 0.0001)

    def test_multipart_geometry(self):
        evalscript = """
            //VERSION=3

            function setup() {
            return {
                input: ["B02", "B03", "B04", "dataMask"],
                output: { id:"default", bands: 3}
            }
            }

            function updateOutputMetadata(scenes, inputMetadata, outputMetadata) {
            var sum = (r_count + g_count + b_count);
            var r_rat = r_count / sum;
            var g_rat = g_count / sum;
            var b_rat = b_count / sum;
            outputMetadata.userData = { "rgb_ratios":  [r_rat, g_rat, b_rat] }
            }

            var r_count = 0;
            var g_count = 0;
            var b_count = 0;

            function evaluatePixel(sample) {

                b_count += sample.B02;
                g_count += sample.B03;
                r_count += sample.B04;

                if (sample.dataMask == 0) {
                    return [1,1,1];
                }

                return [sample.B04*2.5, sample.B03*2.5, sample.B02*2.5];
            }
        """

        points = [(3983945.16471594, 4455475.78793186), (3983888.00256275, 4455757.47439827),
                  (3983881.86585896, 4455756.21837259), (3983806.23113651, 4456128.92147268),
                  (3983795.43856837, 4456181.52922753), (3983782.49665288, 4456243.16761979),
                  (3983769.24786918, 4456304.74059236), (3983755.69254332, 4456366.24660331),
                  (3983741.83100914, 4456427.68411298), (3983731.89973217, 4456470.84795843),
                  (3983633.33670483, 4456895.81023678), (3983639.43692276, 4456897.23726002),
                  (3983537.58701916, 4457336.35298979), (3983531.486563, 4457334.92584801),
                  (3983451.81567033, 4457678.40439185), (3983444.09684707, 4457713.91738361),
                  (3983433.21703553, 4457774.95105094), (3983425.2423303, 4457836.4359853),
                  (3983420.19086095, 4457898.23280495), (3983418.07413054, 4457960.2014162),
                  (3983418.89698951, 4458022.20133071), (3983422.65762374, 4458084.09198476),
                  (3983429.05535825, 4458143.30899922), (3983435.27377231, 4458142.55298424),
                  (3983439.97457434, 4458175.43769638), (3983450.97468474, 4458236.14788553),
                  (3983466.88315168, 4458303.87476693), (3983460.83517165, 4458305.51224157),
                  (3983466.80900589, 4458327.76588705), (3983484.9991527, 4458387.02138291),
                  (3983505.97749719, 4458445.340412), (3983538.67409472, 4458522.43435024),
                  (3983584.70089337, 4458635.18822735), (3983780.40768297, 4459048.67824218),
                  (3983801.72985096, 4459096.84527808), (3983883.42859759, 4459278.64097453),
                  (3984316.01202946, 4460214.51826613), (3984398.97672295, 4460080.53793049),
                  (3984534.50220822, 4459799.86484374), (3984577.77550522, 4459774.02321167),
                  (3984491.40157364, 4459687.94895666), (3984776.22996932, 4459142.13379129),
                  (3984819.68594039, 4459029.12887873), (3984907.71921624, 4458981.665405),
                  (3984888.9490588, 4458770.02890185), (3985209.2168573, 4458503.41559024),
                  (3985821.45298221, 4458006.99923219), (3985788.76207523, 4457880.30735337),
                  (3985793.50611539, 4457877.12247581), (3985784.68739608, 4457859.48509427),
                  (3985732.13693102, 4457697.05635426), (3985820.89433686, 4457656.86419316),
                  (3985677.94930497, 4457315.34906349), (3985611.18897298, 4457337.80151946),
                  (3985327.61285454, 4457451.86990929), (3985146.68294768, 4456972.64460213),
                  (3985446.37981687, 4456852.84034971), (3985488.11295695, 4456837.9565739),
                  (3985384.27368677, 4456550.32595766), (3985005.77351172, 4455718.96868536),
                  (3984372.83691021, 4455665.6888113), (3984231.62160324, 4455623.03272949),
                  (3984096.30921154, 4455487.68759209), (3983945.16471594, 4455475.78793186)]

        sgeo = Polygon(points)
        crs = CRS('epsg:3857')
        geo = Geometry(sgeo, crs=crs)
        bbox = BBox(sgeo.bounds, crs=crs)
        size = bbox_to_dimensions(bbox, 10)

        request = SentinelHubRequest(
            evalscript=evalscript,
            input_data=[
                SentinelHubRequest.input_data(
                    data_source=DataSource.SENTINEL2_L1C,
                    time_interval=('2017-11-15T07:12:03', '2017-12-15T07:12:04'),
                )
            ],
            responses=[
                SentinelHubRequest.output_response('default', MimeType.TIFF),
                SentinelHubRequest.output_response('userdata', MimeType.JSON)
            ],
            bbox=bbox,
            geometry=geo,
            size=size,
            mime_type=MimeType.TAR
        )

        tar = request.get_data(max_threads=3)[0]

        img = tar['default.tif']

        self.assertEqual(img.shape, (382, 181, 3))
        self.test_numpy_data(img, exp_min=25, exp_max=255, exp_mean=144.89)

        json_data = tar['userdata.json']

        self.assertTrue('rgb_ratios' in json_data)

        expected_ratios = [0.29098381560041126, 0.3227735909047216, 0.3862425934948671]
        self.assertTrue(np.allclose(json_data['rgb_ratios'], expected_ratios))


if __name__ == "__main__":
    unittest.main()
