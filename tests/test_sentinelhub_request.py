""" Tests for the Processing API requests
"""
import unittest

from sentinelhub import SentinelHubRequest, CRS, BBox, TestSentinelHub, DataSource, MimeType


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
                    maxcc=0.8
                )
            ],
            responses=[
                SentinelHubRequest.output_response('default', 'image/tiff')
            ],
            bbox=BBox(bbox=[46.16, -16.15, 46.51, -15.58], crs=CRS.WGS84),
            size=(512, 856),
            mime_type=MimeType.TIFF
        )

        img = request.get_data(max_threads=3)

        self.assertEqual(img.shape, (856, 512, 3))
        self.test_numpy_data(img, exp_min=0, exp_max=255, exp_mean=74.92)

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
                        id:"default",
                        bands: 3,
                        sampleType: SampleType.UINT16
                    }
                }
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
                SentinelHubRequest.output_response('default', 'image/tiff'),
                SentinelHubRequest.output_response('userdata', 'application/json')
            ],
            bbox=BBox(bbox=[46.16, -16.15, 46.51, -15.58], crs=CRS.WGS84),
            size=(512, 856),
            mime_type=MimeType.TAR
        )

        tar = request.get_data(max_threads=3)

        self.assertTrue('default.tif' in tar)
        self.assertTrue('userdata.json' in tar)

        img_data = tar['default.tif']

        self.assertEqual(img_data.shape, (856, 512, 3))
        self.test_numpy_data(img_data, exp_min=0, exp_max=8073, exp_mean=1176.32)

        json_data = tar['userdata.json']

        self.assertTrue('norm_factor' in json_data)
        self.assertTrue(json_data['norm_factor'] == 0.0001)


if __name__ == "__main__":
    unittest.main()
