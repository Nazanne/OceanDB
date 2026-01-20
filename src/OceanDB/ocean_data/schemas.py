from typing import Literal

AlongTrackRadiusFields = Literal[
    "latitude",
    "longitude",
    "sla_filtered",
    "distance",
    "delta_t",
]

ProjectedAlongTrackFields = Literal[
    "latitude",
    "longitude",
    "sla_filtered",
    "distance",
    "delta_t",
    "x",
    "y",
    "delta_x",
    "delta_y",
]

EddyTrackFields = Literal[
    "track",
    "cyclonic_type",
    "date_time",
    "latitude",
    "longitude",
    "observation_number",
    "speed_radius",
    "amplitude",
]

AlongTrackNearEddyFields = Literal[
    "file_name",
    "track",
    "cycle",
    "latitude",
    "longitude",
    "time",
    "sla_unfiltered",
    "sla_filtered",
    "dac",
    "ocean_tide",
    "internal_tide",
    "lwe",
    "mdt",
    "tpa_correction",
]