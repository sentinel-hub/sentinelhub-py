"""
Constants related to AWS functionalities of the package
"""
import itertools as it
from enum import Enum

from ..constants import MimeType


class AwsConstants:
    """Initialisation of every constant used by AWS classes

    For each supported data collection it contains lists of all possible bands and all possible metadata files:

        - S2_L1C_BANDS and S2_L1C_METAFILES
        - S2_L2A_BANDS and S2_L2A_METAFILES

    It also contains dictionary of all possible files and their formats: AWS_FILES
    """

    # General constants:
    SOURCE_ID_LIST = ["L1C", "L2A"]
    TILE_INFO = "tileInfo"
    PRODUCT_INFO = "productInfo"
    METADATA = "metadata"
    PREVIEW = "preview"
    PREVIEW_JP2 = "preview*"
    QI_LIST = ["DEFECT", "DETFOO", "NODATA", "SATURA", "TECQUA"]
    QI_LIST_V4 = ["DETFOO", "QUALIT"]
    QI_MSK_CLASSI = "qi/CLASSI_B00"
    QI_MSK_CLOUD = "qi/MSK_CLOUDS_B00"
    AUX_DATA = "AUX_DATA"
    DATASTRIP = "DATASTRIP"
    GRANULE = "GRANULE"
    HTML = "HTML"
    INFO = "rep_info"
    QI_DATA = "QI_DATA"
    IMG_DATA = "IMG_DATA"
    INSPIRE = "inspire"
    MANIFEST = "manifest"
    TCI = "TCI"
    PVI = "PVI"
    CAMSFO = "auxiliary/CAMSFO"
    AUX_CAMSFO = "auxiliary/AUX_CAMSFO"
    ECMWFT = "auxiliary/ECMWFT"
    AUX_ECMWFT = "auxiliary/AUX_ECMWFT"
    DATASTRIP_METADATA = "datastrip/*/metadata"

    # More constants about L2A
    AOT = "AOT"
    WVP = "WVP"
    SCL = "SCL"
    VIS = "VIS"
    L2A_MANIFEST = "L2AManifest"
    REPORT = "report"
    GIPP = "auxiliary/GIP_TL"
    FORMAT_CORRECTNESS = "FORMAT_CORRECTNESS"
    GENERAL_QUALITY = "GENERAL_QUALITY"
    GEOMETRIC_QUALITY = "GEOMETRIC_QUALITY"
    RADIOMETRIC_QUALITY = "RADIOMETRIC_QUALITY"
    SENSOR_QUALITY = "SENSOR_QUALITY"
    L2A_QUALITY = "L2A_QUALITY"
    QUALITY_REPORTS = [FORMAT_CORRECTNESS, GENERAL_QUALITY, GEOMETRIC_QUALITY, RADIOMETRIC_QUALITY, SENSOR_QUALITY]
    CLASS_MASKS = ["SNW", "CLD"]
    R10m = "R10m"
    R20m = "R20m"
    R60m = "R60m"
    RESOLUTIONS = [R10m, R20m, R60m]
    S2_L2A_BAND_MAP = {
        R10m: ["B02", "B03", "B04", "B08", AOT, TCI, WVP],
        R20m: ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B8A", "B11", "B12", AOT, SCL, TCI, VIS, WVP],
        R60m: ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B8A", "B09", "B11", "B12", AOT, SCL, TCI, WVP],
    }

    # Order of elements in following lists is important
    # Sentinel-2 L1C products:
    S2_L1C_BANDS = ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B10", "B11", "B12"]
    S2_L1C_METAFILES = (
        [PRODUCT_INFO, TILE_INFO, METADATA, INSPIRE, MANIFEST, DATASTRIP_METADATA]
        + [f"datastrip/*/qi/{qi_report}_report" for qi_report in QUALITY_REPORTS]
        + [PREVIEW, PREVIEW_JP2, TCI]
        + [f"{preview}/{band}" for preview, band in it.zip_longest([], S2_L1C_BANDS, fillvalue=PREVIEW)]
        + [QI_MSK_CLASSI, QI_MSK_CLOUD]
        + [f"qi/MSK_{qi}_{band}" for qi, band in it.product(QI_LIST, S2_L1C_BANDS)]
        + [f"qi/{qi}_{band}" for qi, band in it.product(QI_LIST_V4, S2_L1C_BANDS)]
        + [f"qi/{qi_report}" for qi_report in [FORMAT_CORRECTNESS, GENERAL_QUALITY, GEOMETRIC_QUALITY, SENSOR_QUALITY]]
        + [CAMSFO, ECMWFT]
    )

    # Sentinel-2 L2A products:
    S2_L2A_BANDS = [
        f"{resolution}/{band}" for resolution, band_list in sorted(S2_L2A_BAND_MAP.items()) for band in band_list
    ]
    S2_L2A_METAFILES = (
        [PRODUCT_INFO, TILE_INFO, METADATA, INSPIRE, MANIFEST, L2A_MANIFEST, REPORT, DATASTRIP_METADATA]
        + [f"datastrip/*/qi/{qi_report}" for qi_report in QUALITY_REPORTS]
        + [f"qi/{source_id}_PVI" for source_id in SOURCE_ID_LIST]
        + [f'qi/{mask}_{res.lstrip("R")}' for mask, res in it.product(CLASS_MASKS, [R20m, R60m])]
        + [f"qi/MSK_{qi}_{band}" for qi, band in it.product(QI_LIST, S2_L1C_BANDS)]
        + [f"qi/{qi}_{band}" for qi, band in it.product(QI_LIST_V4, S2_L1C_BANDS)]
        + [QI_MSK_CLASSI, QI_MSK_CLOUD]
        + [f"qi/{qi_report}" for qi_report in QUALITY_REPORTS + [L2A_QUALITY]]
        + [AUX_CAMSFO, ECMWFT, AUX_ECMWFT, GIPP]
    )

    # Product files with formats:
    PRODUCT_FILES = {
        **{
            PRODUCT_INFO: MimeType.JSON,
            METADATA: MimeType.XML,
            INSPIRE: MimeType.XML,
            MANIFEST: MimeType.SAFE,
            L2A_MANIFEST: MimeType.XML,
            REPORT: MimeType.XML,
            DATASTRIP_METADATA: MimeType.XML,
        },
        **{f"datastrip/*/qi/{qi_report}": MimeType.XML for qi_report in QUALITY_REPORTS},
    }
    # Tile files with formats:
    TILE_FILES = {
        **{
            TILE_INFO: MimeType.JSON,
            PRODUCT_INFO: MimeType.JSON,
            METADATA: MimeType.XML,
            PREVIEW: MimeType.JPG,
            PREVIEW_JP2: MimeType.JP2,
            TCI: MimeType.JP2,
            QI_MSK_CLASSI: MimeType.JP2,
            QI_MSK_CLOUD: MimeType.GML,
            CAMSFO: MimeType.RAW,
            AUX_CAMSFO: MimeType.RAW,
            ECMWFT: MimeType.RAW,
            AUX_ECMWFT: MimeType.RAW,
            GIPP: MimeType.XML,
        },
        **{f"qi/{qi_report}": MimeType.XML for qi_report in QUALITY_REPORTS + [L2A_QUALITY]},
        **{f"{preview}/{band}": MimeType.JP2 for preview, band in it.zip_longest([], S2_L1C_BANDS, fillvalue=PREVIEW)},
        **{f"qi/MSK_{qi}_{band}": MimeType.GML for qi, band in it.product(QI_LIST, S2_L1C_BANDS)},
        **{band: MimeType.JP2 for band in S2_L1C_BANDS},
        **{band: MimeType.JP2 for band in S2_L2A_BANDS},
        **{f"qi/{source_id}_PVI": MimeType.JP2 for source_id in SOURCE_ID_LIST},
        **{f'qi/{mask}_{res.lstrip("R")}': MimeType.JP2 for mask, res in it.product(CLASS_MASKS, [R20m, R60m])},
    }

    # All files joined together
    AWS_FILES = {
        **PRODUCT_FILES,
        **{filename.split("/")[-1]: data_format for filename, data_format in PRODUCT_FILES.items()},
        **TILE_FILES,
        **{filename.split("/")[-1]: data_format for filename, data_format in TILE_FILES.items()},
    }


class EsaSafeType(Enum):
    """Enum constants class for ESA .SAFE type.

    Types are OLD_TYPE and COMPACT_TYPE
    """

    OLD_TYPE = "old_type"
    COMPACT_TYPE = "compact_type"
