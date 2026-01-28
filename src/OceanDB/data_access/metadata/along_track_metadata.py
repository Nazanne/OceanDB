import numpy as np
from OceanDB.data_access.metadata.variable_spec import VariableSpec

ALONG_TRACK_VARIABLES: dict[str, VariableSpec] = {
        "sla_unfiltered": {
            "dtype": np.float64,
            "scale": 0.001,
            "add_offset": None,
            "attrs": {
                "long_name": "...",
                "standard_name": "sea_surface_height_above_sea_level",
                "units": "m",
                "comment": "...",
            },
        },
        "sla_filtered": {
            "dtype": np.float64,
            "scale": 0.001,
            "add_offset": 0.0,
            "fill_value": 32767,
            "attrs": {
                "long_name": "...",
                "standard_name": "sea_surface_height_above_sea_level",
                "units": "m",
                "comment": "...",
            },
        },
        "dac": {
            "dtype": np.float64,
            "scale": 0.001,
            "add_offset": None,
            "attrs": {
                "long_name": "Dynamic Atmospheric Correction",
                "units": "m",
            },
        },
        "time": {
            "dtype": np.float64,
            "scale": None,
            "add_offset": None,
            "attrs": {
                "standard_name": "time",
                "units": "days since 1950-01-01 00:00:00",
                "calendar": "gregorian",
            },
        },
        "track": {
            "dtype": np.int16,
            "scale": None,
            "add_offset": None,
            "attrs": {
                "long_name": "Track in cycle the measurement belongs to",
                "units": "1",
            },
        },
        "cycle": {
            "dtype": np.int16,
            "scale": None,
            "add_offset": None,
            "attrs": {
                "long_name": "Cycle the measurement belongs to",
                "units": "1",
            },
        },
        "ocean_tide": {
            "dtype": np.float64,
            "scale": 0.001,
            "add_offset": None,
            "attrs": {
                "long_name": "Ocean tide model",
                "units": "m",
            },
        },
        "internal_tide": {
            "dtype": np.float64,
            "scale": 0.001,
            "add_offset": None,
            "attrs": {
                "long_name": "Internal tide correction",
                "units": "m",
            },
        },
        "lwe": {
            "dtype": np.float64,
            "scale": 0.001,
            "add_offset": None,
            "attrs": {
                "long_name": "Long wavelength error",
                "units": "m",
            },
        },
        "mdt": {
            "dtype": np.float64,
            "scale": 0.001,
            "add_offset": None,
            "attrs": {
                "standard_name": "sea_surface_height_above_geoid",
                "units": "m",
            },
        },
    }