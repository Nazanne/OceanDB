import numpy as np
from OceanDB.data_access.metadata.variable_spec import VariableSpec

EDDY_VARIABLES: dict[str, VariableSpec] = {

    "amplitude": {
        "dtype": np.uint16,
        "scale": 0.0001,
        "add_offset": 0,
        "attrs": {
            "long_name": "Amplitude",
            "units": "m",
            "comment": (
                "Magnitude of the height difference between the extremum of SSH "
                "within the eddy and the SSH around the effective contour defining "
                "the eddy edge"
            ),
        },
    },

    "cost_association": {
        "dtype": np.float32,
        "attrs": {
            "long_name": "Cost association between two eddies",
            "comment": "Cost value to associate one eddy with the next observation",
        },
    },

    "effective_area": {
        "dtype": np.float32,
        "attrs": {
            "long_name": "Effective area",
            "units": "m^2",
            "comment": "Area enclosed by the effective contour in m^2",
        },
    },

    "effective_contour_height": {
        "dtype": np.float32,
        "attrs": {
            "long_name": "Effective Contour Height",
            "units": "m",
            "comment": "SSH filtered height for effective contour",
        },
    },

    "effective_contour_latitude": {
        "scale": 0.01,
        "add_offset": 0,
        "attrs": {
            "long_name": "Effective Contour Latitudes",
            "units": "degrees_east",
            "comment": "Latitudes of effective contour",
            "axis": "X",
        },
    },

    "effective_contour_longitude": {
        "scale": 0.01,
        "add_offset": 180.0,
        "attrs": {
            "long_name": "Effective Contour Longitudes",
            "units": "degrees_east",
            "comment": "Longitudes of the effective contour",
            "axis": "X",
        },
    },

    "effective_contour_shape_error": {
        "dtype": np.uint8,
        "scale": 0.5,
        "add_offset": 0,
        "attrs": {
            "long_name": "Effective Contour Shape Error",
            "units": "%",
            "comment": (
                "Error criterion between the effective contour and its best fit circle"
            ),
        },
    },

    "effective_radius": {
        "dtype": np.uint16,
        "scale": 50.0,
        "add_offset": 0,
        "attrs": {
            "long_name": "Effective Radius",
            "units": "m",
            "comment": (
                "Radius of the best fit circle corresponding to the effective contour"
            ),
        },
    },

    "inner_contour_height": {
        "dtype": np.float32,
        "attrs": {
            "long_name": "Inner Contour Height",
            "units": "m",
            "comment": "SSH filtered height for the smallest detected contour",
        },
    },

    "latitude": {
        "dtype": np.float32,
        "attrs": {
            "long_name": "Eddy Center Latitude",
            "standard_name": "latitude",
            "units": "degrees_north",
            "comment": "Latitude center of the best fit circle",
            "axis": "Y",
        },
    },

    "latitude_max": {
        "dtype": np.float32,
        "attrs": {
            "long_name": "Latitude of the SSH maximum",
            "standard_name": "latitude",
            "units": "degrees_north",
            "comment": "Latitude of the inner contour",
            "axis": "Y",
        },
    },

    "longitude": {
        "dtype": np.float32,
        "attrs": {
            "long_name": "Eddy Center Longitude",
            "standard_name": "longitude",
            "units": "degrees_east",
            "comment": "Longitude center of the best fit circle",
            "axis": "X",
        },
    },

    "longitude_max": {
        "dtype": np.float32,
        "attrs": {
            "long_name": "Longitude of the SSH maximum",
            "standard_name": "longitude",
            "units": "degrees_east",
            "comment": "Longitude of the inner contour",
            "axis": "X",
        },
    },

    "num_contours": {
        "dtype": np.uint16,
        "attrs": {
            "long_name": "Number of contours",
            "comment": "Number of contours selected for this eddy",
        },
    },

    "num_point_e": {
        "dtype": np.uint16,
        "attrs": {
            "long_name": "Number of points for effective contour",
            "units": "ordinal",
            "description": (
                "Number of points for effective contour before resampling"
            ),
        },
    },

    "num_point_s": {
        "dtype": np.uint16,
        "attrs": {
            "long_name": "Number of points for speed contour",
            "units": "ordinal",
            "description": (
                "Number of points for speed contour before resampling"
            ),
        },
    },

    "observation_flag": {
        "dtype": np.int8,
        "attrs": {
            "long_name": "Virtual Eddy Position",
            "comment": (
                "Flag indicating if the value is interpolated between two observations "
                "(0: observed eddy, 1: interpolated eddy)"
            ),
        },
    },

    "observation_number": {
        "dtype": np.uint16,
        "attrs": {
            "long_name": "Eddy temporal index in a trajectory",
            "comment": (
                "Observation sequence number, days starting at the eddy first detection"
            ),
        },
    },

    "speed_area": {
        "dtype": np.float32,
        "attrs": {
            "long_name": "Speed area",
            "units": "m^2",
            "comment": "Area enclosed by the speed contour in m^2",
        },
    },

    "speed_average": {
        "dtype": np.uint16,
        "scale": 0.0001,
        "add_offset": 0,
        "attrs": {
            "long_name": "Maximum circum-averaged Speed",
            "units": "m/s",
            "comment": (
                "Average speed of the contour defining the radius scale speed_radius"
            ),
        },
    },

    "speed_contour_height": {
        "dtype": np.float32,
        "attrs": {
            "long_name": "Speed Contour Height",
            "units": "m",
            "comment": "SSH filtered height for speed contour",
        },
    },

    "speed_contour_latitude": {
        "dtype": np.int16,
        "scale": 0.01,
        "add_offset": 0,
        "attrs": {
            "long_name": "Speed Contour Latitudes",
            "units": "degrees_east",
            "comment": "Latitudes of speed contour",
            "axis": "X",
        },
    },

    "speed_contour_longitude": {
        "dtype": np.int16,
        "scale": 0.01,
        "add_offset": 180.0,
        "attrs": {
            "long_name": "Speed Contour Longitudes",
            "units": "degrees_east",
            "comment": "Longitudes of speed contour",
            "axis": "X",
        },
    },

    "speed_contour_shape_error": {
        "dtype": np.uint8,
        "scale": 0.5,
        "add_offset": 0,
        "attrs": {
            "long_name": "Speed Contour Shape Error",
            "units": "%",
            "comment": (
                "Error criterion between the speed contour and its best fit circle"
            ),
        },
    },

    "speed_radius": {
        "dtype": np.uint16,
        "scale": 50.0,
        "add_offset": 0,
        "attrs": {
            "long_name": "Speed Radius",
            "units": "m",
            "comment": (
                "Radius of the best fit circle corresponding to the contour of maximum "
                "circum-average speed"
            ),
        },
    },

    "time": {
        "scale": 1.15740740740741e-05,
        "add_offset": 0,
        "attrs": {
            "long_name": "Time",
            "standard_name": "time",
            "units": "days since 1950-01-01 00:00:00",
            "calendar": "proleptic_gregorian",
            "comment": "Date of this observation",
            "axis": "T",
        },
    },

    "track": {
        "dtype": np.uint32,
        "attrs": {
            "long_name": "Trajectory number",
            "comment": "Trajectory identification number",
        },
    },

    "uavg_profile": {
        "dtype": np.uint16,
        "scale": 0.0001,
        "add_offset": 0,
        "attrs": {
            "long_name": "Radial Speed Profile",
            "units": "m/s",
            "comment": (
                "Speed averaged values from the effective contour inwards to the "
                "smallest contour, evenly spaced points"
            ),
        },
    },
}