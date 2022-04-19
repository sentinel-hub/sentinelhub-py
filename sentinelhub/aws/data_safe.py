"""
Module for creating .SAFE structure with data collected from AWS
"""

import warnings

from ..constants import MimeType
from ..data_collections import DataCollection
from ..exceptions import SHRuntimeWarning
from .client import AwsDownloadClient
from .constants import AwsConstants, EsaSafeType
from .data import AwsProduct, AwsTile


class SafeProduct(AwsProduct):
    """Class implementing transformation of Sentinel-2 satellite products from AWS into .SAFE structure"""

    def get_requests(self):
        """Creates product structure and returns list of files for download

        :return: list of download requests
        :rtype: list(download.DownloadRequest)
        """
        safe = self.get_safe_struct()

        self.download_list = []
        self.structure_recursion(safe, self.parent_folder)
        self.sort_download_list()
        return self.download_list, self.folder_list

    def get_safe_struct(self):
        """Describes a structure inside tile folder of ESA product .SAFE structure

        :return: nested dictionaries representing .SAFE structure
        :rtype: dict
        """
        main_folder = self.get_main_folder()
        safe = {
            main_folder: {
                AwsConstants.AUX_DATA: {},
                AwsConstants.DATASTRIP: self._get_datastrip_substruct(),
                AwsConstants.GRANULE: self._get_granule_substruct(),
                self.get_product_metadata_name(): self.get_url(AwsConstants.METADATA),
                "INSPIRE.xml": self.get_url(AwsConstants.INSPIRE),
                self.add_file_extension(AwsConstants.MANIFEST): self.get_url(AwsConstants.MANIFEST),
                AwsConstants.HTML: {},  # AWS doesn't have this data
                AwsConstants.INFO: {},  # AWS doesn't have this data
            }
        }

        if self.is_early_compact_l2a():
            safe[main_folder]["L2A_Manifest.xml"] = self.get_url(AwsConstants.L2A_MANIFEST)
            safe[main_folder][self.get_report_name()] = self.get_url(AwsConstants.REPORT)

        if self.safe_type is EsaSafeType.OLD_TYPE and self.baseline != "02.02":
            safe[main_folder][_edit_name(self.product_id, "BWI") + ".png"] = self.get_url(
                AwsConstants.PREVIEW, MimeType.PNG
            )
        return safe

    def _get_datastrip_substruct(self):
        """Builds a datastrip subfolder structure of .SAFE format."""
        datastrip_safe = {}
        datastrip_list = self.get_datastrip_list()
        for datastrip_folder, datastrip_url in datastrip_list:
            datastrip_safe[datastrip_folder] = {AwsConstants.QI_DATA: {}}

            if self.has_reports():
                for metafile in AwsConstants.QUALITY_REPORTS:
                    metafile_name = self.add_file_extension(metafile)

                    metafile_s3_name = metafile_name
                    # S-2 L1C reports are available on S3 under modified names
                    if self.data_collection is DataCollection.SENTINEL2_L1C:
                        metafile_s3_name = metafile_s3_name.replace(".xml", "_report.xml")

                    metafile_url = f"{datastrip_url}/qi/{metafile_s3_name}"
                    datastrip_safe[datastrip_folder][AwsConstants.QI_DATA][metafile_name] = metafile_url

            datastrip_name = self.get_datastrip_metadata_name(datastrip_folder)
            datastrip_url = f"{datastrip_url}/{self.add_file_extension(AwsConstants.METADATA)}"
            datastrip_safe[datastrip_folder][datastrip_name] = datastrip_url

        return datastrip_safe

    def _get_granule_substruct(self):
        """Builds a granule subfolder structure of .SAFE format."""
        granule_safe = {}
        for tile_info in self.product_info["tiles"]:
            tile_name, date, aws_index = self.url_to_tile(self.get_tile_url(tile_info))

            if self.tile_list is None or AwsTile.parse_tile_name(tile_name) in self.tile_list:
                tile_struct = SafeTile(
                    tile_name,
                    date,
                    aws_index,
                    parent_folder=None,
                    bands=self.bands,
                    metafiles=self.metafiles,
                    data_collection=self.data_collection,
                ).get_safe_struct()

                granule_safe.update(tile_struct)

        return granule_safe

    def get_main_folder(self):
        """
        :return: name of main folder
        :rtype: str
        """
        return f"{self.product_id}.SAFE"

    def get_datastrip_list(self):
        """
        :return: list of datastrips folder names and urls from `productInfo.json` file
        :rtype: list((str, str))
        """
        datastrips = self.product_info["datastrips"]
        return [
            (self.get_datastrip_name(datastrip["id"]), f'{self.base_url}/{datastrip["path"]}')
            for datastrip in datastrips
        ]

    def get_datastrip_name(self, datastrip):
        """
        :param datastrip: name of datastrip
        :type datastrip: str
        :return: name of datastrip folder
        :rtype: str
        """
        if self.safe_type is EsaSafeType.OLD_TYPE:
            return datastrip
        return "_".join(datastrip.split("_")[4:-1])

    def get_datastrip_metadata_name(self, datastrip_folder):
        """
        :param datastrip_folder: name of datastrip folder
        :type datastrip_folder: str
        :return: name of datastrip metadata file
        :rtype: str
        """
        if self.safe_type is EsaSafeType.OLD_TYPE:
            name = _edit_name(datastrip_folder, "MTD", delete_end=True)
        else:
            name = "MTD_DS"
        return f"{name}.{MimeType.XML.value}"

    def get_product_metadata_name(self):
        """
        :return: name of product metadata file
        :rtype: str
        """
        if self.safe_type is EsaSafeType.OLD_TYPE:
            name = _edit_name(self.product_id, "MTD", "SAFL1C")
        else:
            name = f'MTD_{self.product_id.split("_")[1]}'
        return f"{name}.{MimeType.XML.value}"

    def get_report_name(self):
        """
        :return: name of the report file of L2A products
        :rtype: str
        """
        return f"{self.product_id}_{self.get_report_time()}_report.{MimeType.XML.value}"

    def get_report_time(self):
        """Returns time when the L2A processing started and reports was created.
        :return: String in a form YYYYMMDDTHHMMSS
        :rtype: str
        """
        client = AwsDownloadClient(config=self.config)
        tree = client.get_xml(self.get_url(AwsConstants.REPORT))

        try:
            timestamp = tree.find("check/inspection").attrib["execution"]
            return timestamp.split(",")[0].replace(" ", "T").replace(":", "").replace("-", "")
        except AttributeError:
            warnings.warn("Could not obtain the L2A report creation time", category=SHRuntimeWarning)
            return "unknown"


class SafeTile(AwsTile):
    """Class implementing transformation of Sentinel-2 satellite tiles from AWS into .SAFE structure"""

    def __init__(self, *args, **kwargs):
        """Initialization parameters are inherited from parent class"""
        super().__init__(*args, **kwargs)

        self.tile_id = self.get_tile_id()

    def get_requests(self):
        """Creates tile structure and returns list of files for download.

        :return: list of download requests for
        :rtype: list(download.DownloadRequest)
        """
        safe = self.get_safe_struct()

        self.download_list = []
        self.structure_recursion(safe, self.parent_folder)
        self.sort_download_list()
        return self.download_list, self.folder_list

    def get_safe_struct(self):
        """Describes a structure inside tile folder of ESA product .SAFE structure.

        :return: nested dictionaries representing .SAFE structure
        :rtype: dict
        """
        return {
            self.get_main_folder(): {
                AwsConstants.AUX_DATA: self._get_aux_substruct(),
                AwsConstants.IMG_DATA: self._get_image_substruct(),
                AwsConstants.QI_DATA: self._get_qi_substruct(),
                self.get_tile_metadata_name(): self.get_url(AwsConstants.METADATA),
            }
        }

    def _get_aux_substruct(self):
        """Builds an auxiliary data subfolder structure of .SAFE format.

        Note: Old products also have DEM and MSI in aux folder which are not reconstructed here.
        """
        aux_safe = {}

        # Not sure if 2nd condition of the following is correct:
        if self.data_collection is DataCollection.SENTINEL2_L2A or self.baseline != "02.04":
            ecmwft_file = (
                AwsConstants.ECMWFT
                if self.data_collection is DataCollection.SENTINEL2_L1C or self.safe_type is EsaSafeType.OLD_TYPE
                else AwsConstants.AUX_ECMWFT
            )
            aux_safe[self.get_aux_data_name()] = self.get_url(ecmwft_file)

            if self.baseline >= "04.00":
                camsfo_file = (
                    AwsConstants.CAMSFO
                    if self.data_collection is DataCollection.SENTINEL2_L1C
                    else AwsConstants.AUX_CAMSFO
                )
                aux_safe["AUX_CAMSFO"] = self.get_url(camsfo_file)

        if self.is_early_compact_l2a():
            gipp_filename = self.add_file_extension(AwsConstants.GIPP, remove_path=True)
            aux_safe[gipp_filename] = self.get_url(AwsConstants.GIPP)

        return aux_safe

    def _get_image_substruct(self):
        """Builds the part of structure of .SAFE format that contains satellite imagery."""
        img_safe = {}

        if self.data_collection is DataCollection.SENTINEL2_L1C:
            for band in self.bands:
                img_safe[self.get_img_name(band)] = self.get_url(band)

            if self.safe_type is EsaSafeType.COMPACT_TYPE:
                img_safe[self.get_img_name(AwsConstants.TCI)] = self.get_url(AwsConstants.TCI)

            return img_safe

        for resolution in AwsConstants.RESOLUTIONS:
            img_safe[resolution] = {}

        for band_name in self.bands:
            resolution, band = band_name.split("/")
            if self._band_exists(band_name):
                img_safe[resolution][self.get_img_name(band, resolution)] = self.get_url(band_name)

        return img_safe

    def _get_qi_substruct(self):
        """Builds a quality-indicators data subfolder structure of .SAFE format."""
        qi_safe = self._get_reports_substruct()

        if self.baseline >= "04.00":
            qi_list = AwsConstants.QI_LIST_V4
            qi_data_format = MimeType.JP2
            clouds_qi_name = "CLASSI"
        else:
            qi_list = AwsConstants.QI_LIST
            qi_data_format = MimeType.GML
            clouds_qi_name = "CLOUDS"

        clouds_qi_filename = self.get_qi_name(clouds_qi_name, data_format=qi_data_format)
        qi_safe[clouds_qi_filename] = self.get_band_qi_url(clouds_qi_name, data_format=qi_data_format)

        for qi_type in qi_list:
            for band in AwsConstants.S2_L1C_BANDS:
                band_qi_filename = self.get_qi_name(qi_type, band, data_format=qi_data_format)
                qi_safe[band_qi_filename] = self.get_band_qi_url(qi_type, band, data_format=qi_data_format)

        if self.data_collection is DataCollection.SENTINEL2_L2A:
            for mask in AwsConstants.CLASS_MASKS:
                for resolution in [AwsConstants.R20m, AwsConstants.R60m]:
                    if "00.01" < self.baseline <= "02.06":
                        mask_name = self.get_img_name(mask, resolution)
                    else:
                        mask_name = self.get_qi_name(f"{mask}PRB", resolution.lstrip("R"), MimeType.JP2)
                    qi_safe[mask_name] = self.get_qi_url(f'{mask}_{resolution.lstrip("R")}.jp2')

        if self.is_early_compact_l2a():
            qi_safe[self.get_img_name(AwsConstants.PVI)] = self.get_preview_url("L2A")

        is_newer_l2a_version = self.data_collection is DataCollection.SENTINEL2_L2A and (
            self.baseline >= "02.07" or self.baseline == "00.01"
        )
        preview_type = "L2A" if is_newer_l2a_version else "L1C"
        qi_safe[self.get_preview_name()] = self.get_preview_url(preview_type)

        return qi_safe

    def _get_reports_substruct(self):
        """Builds a substructure of .SAFE format with reports."""
        reports_safe = {}
        if not self.has_reports():
            return reports_safe

        reports = AwsConstants.QUALITY_REPORTS
        if self.data_collection is DataCollection.SENTINEL2_L2A and self.baseline >= "04.00":
            reports = reports + [AwsConstants.L2A_QUALITY]

        for metafile in reports:
            if (
                metafile == AwsConstants.RADIOMETRIC_QUALITY
                and self.data_collection is DataCollection.SENTINEL2_L2A
                and (self.baseline <= "02.07" or self.baseline >= "04.00")
            ):
                continue

            metafile_name = self.add_file_extension(metafile, remove_path=True)
            reports_safe[metafile_name] = self.get_qi_url(metafile_name)

        return reports_safe

    def get_tile_id(self):
        """Creates ESA tile ID

        :return: ESA tile ID
        :rtype: str
        """
        client = AwsDownloadClient(config=self.config)
        tree = client.get_xml(self.get_url(AwsConstants.METADATA))

        tile_id_tag = (
            "TILE_ID_2A"
            if (self.data_collection is DataCollection.SENTINEL2_L2A and "00.01" < self.baseline <= "02.06")
            else "TILE_ID"
        )
        tile_id = tree[0].find(tile_id_tag).text
        if self.safe_type is EsaSafeType.OLD_TYPE:
            return tile_id

        info = tile_id.split("_")

        if (self.data_collection is DataCollection.SENTINEL2_L1C and self.baseline >= "02.07") or (
            self.data_collection is DataCollection.SENTINEL2_L2A
            and (self.baseline >= "02.10" or self.baseline == "00.01")
        ):
            tile_id_time = self.get_datastrip_time()
        else:
            tile_id_time = self.get_sensing_time()

        return "_".join([info[3], info[-2], info[-3], tile_id_time])

    def get_sensing_time(self):
        """
        :return: Exact tile sensing time
        :rtype: str
        """
        return self.tile_info["timestamp"].split(".")[0].replace("-", "").replace(":", "")

    def get_datastrip_time(self):
        """
        :return: Exact datastrip time
        :rtype: str
        """
        # S2A_OPER_MSI_L1C_DS_EPAE_20181119T061056_S20181119T031012_N02.07 -> 20181119T031012
        # S2A_OPER_MSI_L1C_DS_EPA__20190225T132350_S20190129T144524_N02.07 -> 20190129T144524
        return self.tile_info["datastrip"]["id"].replace("__", "_").split("_")[7].lstrip("S")

    def get_datatake_time(self):
        """
        :return: Exact time of datatake
        :rtype: str
        """
        return self.tile_info["productName"].split("_")[2]

    def get_main_folder(self):
        """
        :return: name of tile folder
        :rtype: str
        """
        return self.tile_id

    def get_tile_metadata_name(self):
        """
        :return: name of tile metadata file
        :rtype: str
        """
        if self.safe_type is EsaSafeType.OLD_TYPE:
            name = _edit_name(self.tile_id, "MTD", delete_end=True)
        else:
            name = "MTD_TL"
        return f"{name}.xml"

    def get_aux_data_name(self):
        """
        :return: name of auxiliary data file
        :rtype: str
        """
        if self.safe_type is EsaSafeType.OLD_TYPE:
            # this is not correct, but we cannot reconstruct last two timestamps in auxiliary data file name
            # e.g. S2A_OPER_AUX_ECMWFT_EPA__20160120T231011_V20160103T150000_20160104T030000
            return "AUX_ECMWFT"
        return "AUX_ECMWFT"

    def get_img_name(self, band, resolution=None):
        """
        :param band: band name
        :type band: str
        :param resolution: Specifies the resolution in case of Sentinel-2 L2A products
        :type resolution: str or None
        :return: name of band image file
        :rtype: str
        """
        band = band.split("/")[-1]
        if self.safe_type is EsaSafeType.OLD_TYPE:
            name = self.tile_id.rsplit("_", 1)[0] + "_" + band
        else:
            name = "_".join([self.tile_id.split("_")[1], self.get_datatake_time(), band])
        if self.data_collection is DataCollection.SENTINEL2_L2A and resolution is not None:
            name = f'{name}_{resolution.lstrip("R")}'
        if self.data_collection is DataCollection.SENTINEL2_L2A and "00.01" < self.baseline <= "02.06":
            name = f"L2A_{name}"
        return f"{name}.jp2"

    def get_qi_name(self, qi_type, band="B00", data_format=MimeType.GML):
        """
        :param qi_type: type of quality indicator
        :type qi_type: str
        :param band: band name
        :type band: str
        :param data_format: format of the file
        :type data_format: MimeType
        :return: name of gml file
        :rtype: str
        """
        band = band.split("/")[-1]
        if self.safe_type is EsaSafeType.OLD_TYPE:
            name = _edit_name(self.tile_id, "MSK", delete_end=True)
            collection_param = f"{'L1C' if self.data_collection is DataCollection.SENTINEL2_L1C else 'L2A'}_TL"
            name = name.replace(collection_param, qi_type)
            name = f"{name}_{band}_MSIL1C"
        else:
            name = f"MSK_{qi_type}_{band}"
        return f"{name}.{data_format.value}"

    def get_preview_name(self):
        """Returns .SAFE name of full resolution L1C preview
        :return: name of preview file
        :rtype: str
        """
        if self.safe_type is EsaSafeType.OLD_TYPE:
            name = _edit_name(self.tile_id, AwsConstants.PVI, delete_end=True)
        else:
            name = "_".join([self.tile_id.split("_")[1], self.get_datatake_time(), AwsConstants.PVI])
        return f"{name}.jp2"


def _edit_name(name, code, add_code=None, delete_end=False):
    """Helping function for creating file names in .SAFE format

    :param name: initial string
    :type name: str
    :param code:
    :type code: str
    :param add_code:
    :type add_code: str or None
    :param delete_end:
    :type delete_end: bool
    :return: edited string
    :rtype: str
    """
    info = name.split("_")
    info[2] = code
    if add_code is not None:
        info[3] = add_code
    if delete_end:
        info.pop()
    return "_".join(info)
