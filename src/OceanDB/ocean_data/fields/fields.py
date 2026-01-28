from ...ocean_data.ocean_data import OceanDataField

import numpy as np
from datetime import datetime

latitude = OceanDataField(
    nc_name="latitude",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="double precision",
    postgres_column_or_query_name="latitude",
)

longitude = OceanDataField(
    nc_name="longitude",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="double precision",
    postgres_column_or_query_name="longitude",
)

date_time = OceanDataField(
    nc_name="time",
    nc_scale=1,
    nc_offset=0,
    python_type=datetime,
    postgres_type="timestamp",
    postgres_column_or_query_name="date_time",
)

file_name = OceanDataField(
    nc_name="file_name",
    nc_scale=1,
    nc_offset=0,
    python_type=str,
    postgres_type="text",
    postgres_column_or_query_name="file_name",
)

mission = OceanDataField(
    nc_name="mission",
    nc_scale=1,
    nc_offset=0,
    python_type=str,
    postgres_type="text",
    postgres_column_or_query_name="mission",
)

track = OceanDataField(
    nc_name="track",
    nc_scale=1,
    nc_offset=0,
    python_type=int,
    postgres_type="smallint",
    postgres_column_or_query_name="track",
)

cycle = OceanDataField(
    nc_name="cycle",
    nc_scale=1,
    nc_offset=0,
    python_type=int,
    postgres_type="smallint",
    postgres_column_or_query_name="cycle",
)

basin_id = OceanDataField(
    nc_name="basin_id",
    nc_scale=1,
    nc_offset=0,
    python_type=int,
    postgres_type="smallint",
    postgres_column_or_query_name="basin_id",
)

sla_unfiltered = OceanDataField(
    nc_name="sla_unfiltered",
    nc_scale=1000,  # meters → millimeters
    nc_offset=0,
    python_type=np.floating,
    postgres_type="smallint",
    postgres_column_or_query_name="sla_unfiltered",
)

sla_filtered = OceanDataField(
    nc_name="sla_filtered",
    nc_scale=1000,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="smallint",
    postgres_column_or_query_name="sla_filtered",
)

dac = OceanDataField(
    nc_name="dac",
    nc_scale=1000,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="smallint",
    postgres_column_or_query_name="dac",
)

ocean_tide = OceanDataField(
    nc_name="ocean_tide",
    nc_scale=1000,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="smallint",
    postgres_column_or_query_name="ocean_tide",
)

internal_tide = OceanDataField(
    nc_name="internal_tide",
    nc_scale=1000,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="smallint",
    postgres_column_or_query_name="internal_tide",
)

lwe = OceanDataField(
    nc_name="lwe",
    nc_scale=1000,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="smallint",
    postgres_column_or_query_name="lwe",
)

mdt = OceanDataField(
    nc_name="mdt",
    nc_scale=1000,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="smallint",
    postgres_column_or_query_name="mdt",
)

tpa_correction = OceanDataField(
    nc_name="tpa_correction",
    nc_scale=1000,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="smallint",
    postgres_column_or_query_name="tpa_correction",
)

distance = OceanDataField(
    nc_name="distance",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="double precision",
    postgres_column_or_query_name="distance",
    custom_calculation="ST_Distance(ST_MakePoint(%(longitude)s, %(latitude)s),along_track_point)",
)

delta_t = OceanDataField(
    nc_name="delta_t",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="double precision",
    postgres_column_or_query_name="delta_t",
    custom_calculation="EXTRACT(EPOCH FROM (%(central_date_time)s - date_time))",
)
