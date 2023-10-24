"""
Module defining evalscript generation utilities
"""

from __future__ import annotations

import numpy as np

from .data_collections import DataCollection
from .data_collections_bands import Band, Unit

DTYPE_TO_SAMPLE_TYPE: dict[type, str] = {
    bool: "UINT8",
    np.uint8: "UINT8",
    np.uint16: "UINT16",
    np.float32: "FLOAT32",
}

EVALSCRIPT_TEMPLATE = """
//VERSION=3

function setup() {{
    return {{
        input: [{{
            bands: [{input_names}],
            units: [{input_units}]
        }}],
        output: [{output_spec}]
    }}
}}

function updateOutputMetadata(scenes, inputMetadata, outputMetadata) {{
    outputMetadata.userData = {{
        "norm_factor":  inputMetadata.normalizationFactor
    }}
}}

function evaluatePixel(sample) {{
    return {{ {return_spec} }};
}}
"""


def parse_data_collection_bands(data_collection: DataCollection, bands: list[str]) -> list[Band]:
    """Checks that all requested bands are available and returns the band information for further processing

    :param data_collection: A collection of requested satellite data.
    :param bands: A list of band or meta band names to use in the evalscript.
    """

    requested_bands = []
    band_info_dict = {band_info.name: band_info for band_info in data_collection.bands + data_collection.metabands}

    for band_name in bands:
        if band_name not in band_info_dict:
            raise ValueError(
                f"Data collection {data_collection} does not have specifications for band {band_name}.\n"
                f"Available bands are:\n{[band.name for band in data_collection.bands]}\nand meta-bands:\n"
                f"{[band.name for band in data_collection.metabands]}."
            )
        requested_bands.append(band_info_dict[band_name])

    return requested_bands


def generate_evalscript(
    data_collection: DataCollection,
    bands: list[str] | None = None,
    meta_bands: list[str] | None = None,
    merged_bands_output: str | None = None,
    prioritize_dn: bool = True,
) -> str:
    """Generate an evalscript based on the provided specifications. This utility supports generating only evalscripts
    with the mosaicking option set to `SIMPLE`.

    :param data_collection: A collection of requested satellite data.
    :param bands: A list of band names to use in the evalscript. Defaults to using all bands provided by the collection.
    :param meta_bands: A list of meta band names to use in the evalscript. By default no meta bands are added.
    :param merged_bands_output: If provided, bands will be concatenated into a single multi-band tiff with this name.
    :param prioritize_dn: Use DN units if possible. Default is True. If DN units are not available, the default units
        for each specific band are used. DN units will be used regardless of the flag if they are the only possible
        choice.
    """

    band_names = bands if bands is not None else [band.name for band in data_collection.bands]
    meta_band_names = meta_bands if meta_bands is not None else []

    input_names, input_units = [], []
    sample_type_map: dict[str, str] = {}
    requested_bands = parse_data_collection_bands(data_collection, band_names + meta_band_names)

    for band in requested_bands:
        unit_choice_idx = band.units.index(Unit.DN) if (prioritize_dn and Unit.DN in band.units) else 0
        sample_type_map[band.name] = DTYPE_TO_SAMPLE_TYPE[band.output_types[unit_choice_idx]]
        input_names.append(f'"{band.name}"')
        input_units.append(f'"{band.units[unit_choice_idx].value}"')

    output_spec, return_spec = [], []
    if merged_bands_output is not None:
        sample_type = sample_type_map[band_names[0]]
        output_spec.append(f'{{id: "{merged_bands_output}", bands: {len(band_names)}, sampleType: "{sample_type}"}}')
        return_spec.append(f"{merged_bands_output}: [{', '.join(f'sample.{band_name}' for band_name in band_names)}]")

    for band_name, sample_type in sample_type_map.items():
        # potentially skip bands if merged bands output specified, but still run for meta bands
        if merged_bands_output is not None and band_name in band_names:
            continue

        output_spec.append(f'{{id: "{band_name}", bands: 1, sampleType: "{sample_type}"}}')
        return_spec.append(f"{band_name}: [sample.{band_name}]")

    return EVALSCRIPT_TEMPLATE.format(
        input_names=", ".join(input_names),
        input_units=", ".join(input_units),
        output_spec=", ".join(output_spec),
        return_spec=", ".join(return_spec),
    )
