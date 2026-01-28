from typing import Literal

from OceanDB.ocean_data.fields import fields
from OceanDB.ocean_data.ocean_data import OceanDataField

along_track_fields = Literal[
    "latitude",
    "longitude",
    "date_time",
    "file_name",
    "mission",
    "track",
    "cycle",
    "basin_id",
    "sla_unfiltered",
    "sla_filtered",
    "dac",
    "ocean_tide",
    "internal_tide",
    "lwe",
    "mdt",
    "tpa_correction",
    "distance",
    "delta_t",
]

along_track_schema: dict[along_track_fields, OceanDataField] = {
    "latitude": fields.latitude,
    "longitude": fields.longitude,
    "date_time": fields.date_time,
    "file_name": fields.file_name,
    "mission": fields.mission,
    "track": fields.track,
    "cycle": fields.cycle,
    "basin_id": fields.basin_id,
    "sla_unfiltered": fields.sla_unfiltered,
    "sla_filtered": fields.sla_filtered,
    "dac": fields.dac,
    "ocean_tide": fields.ocean_tide,
    "internal_tide": fields.internal_tide,
    "lwe": fields.lwe,
    "mdt": fields.mdt,
    "tpa_correction": fields.tpa_correction,
    "distance": fields.distance,
    "delta_t": fields.delta_t,
}
