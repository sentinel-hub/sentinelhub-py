"""
Module implementing some common utility functions
"""
from dataclasses_json import LetterCase
from dataclasses_json import config as dataclass_config

from ..geometry import Geometry
from ..time_utils import parse_time, serialize_time

datetime_config = dataclass_config(
    encoder=lambda time: serialize_time(time, use_tz=True) if time else None,
    decoder=lambda time: parse_time(time, force_datetime=True) if time else None,
    letter_case=LetterCase.CAMEL,
)


geometry_config = dataclass_config(
    encoder=Geometry.get_geojson,
    decoder=lambda geojson: Geometry.from_geojson(geojson) if geojson else None,
    exclude=lambda geojson: geojson is None,
    letter_case=LetterCase.CAMEL,
)


def enum_config(enum_class):
    """Given an Enum class it provide an object for serialization/deserialization"""
    return dataclass_config(
        encoder=lambda enum_item: enum_item.value,
        decoder=lambda item: enum_class(item) if item else None,
        exclude=lambda item: item is None,
        letter_case=LetterCase.CAMEL,
    )


def _update_other_args(dict1, dict2):
    """Function for a recursive update of `dict1` with `dict2`. The function loops over the keys in `dict2` and
    only the non-dict like values are assigned to the specified keys.
    """
    for key, value in dict2.items():
        if isinstance(value, dict) and key in dict1:
            _update_other_args(dict1[key], value)
        else:
            dict1[key] = value


def remove_undefined(payload):
    """Takes a dictionary and removes keys without value"""
    return {name: value for name, value in payload.items() if value is not None}
