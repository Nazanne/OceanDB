import numpy as np

from OceanDB.data_access.metadata.variable_spec import VariableSpec


EDDY_VARIABLES: dict[str, VariableSpec] = {
    "track": {
        "dtype": np.int32,
        "attrs": {
            "long_name": "Eddy track identifier",
        },
    },
    "cyclonic_type": {
        "dtype": np.int8,
        "attrs": {
            "long_name": "Cyclonic (1) or anticyclonic (0)",
        },
    },
    "date_time": {
        "dtype": np.float64,
        "attrs": {
            "standard_name": "time",
            "units": "days since 1950-01-01 00:00:00",
            "calendar": "gregorian",
        },
    },
    "latitude": {
        "dtype": np.float64,
        "attrs": {
            "standard_name": "latitude",
            "units": "degrees_north",
        },
    },
    "longitude": {
        "dtype": np.float64,
        "attrs": {
            "standard_name": "longitude",
            "units": "degrees_east",
        },
    },
    "observation_number": {
        "dtype": np.int32,
        "attrs": {
            "long_name": "Observation index along eddy track",
        },
    },
    "speed_radius": {
        "dtype": np.float64,
        "scale": 50.0,
        "attrs": {
            "long_name": "Speed-based eddy radius",
            "units": "m",
        },
    },
    "amplitude": {
        "dtype": np.float64,
        "scale": 0.0001,
        "attrs": {
            "long_name": "Eddy amplitude",
            "units": "m",
        },
    },
}
