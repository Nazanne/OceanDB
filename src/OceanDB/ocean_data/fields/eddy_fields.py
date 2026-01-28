import numpy as np
from datetime import datetime
from OceanDB.ocean_data.ocean_data import OceanDataField


latitude = OceanDataField(
    nc_name="latitude",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="real",
    postgres_column_or_query_name="latitude",
)

longitude = OceanDataField(
    nc_name="longitude",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="real",
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

track = OceanDataField(
    nc_name="track",
    nc_scale=1,
    nc_offset=0,
    python_type=int,
    postgres_type="integer",
    postgres_column_or_query_name="track",
)

cyclonic_type = OceanDataField(
    nc_name="cyclonic_type",
    nc_scale=1,
    nc_offset=0,
    python_type=int,
    postgres_type="smallint",
    postgres_column_or_query_name="cyclonic_type",
)

amplitude = OceanDataField(
    nc_name="amplitude",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="smallint",
    postgres_column_or_query_name="amplitude",
)

effective_radius = OceanDataField(
    nc_name="effective_radius",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="smallint",
    postgres_column_or_query_name="effective_radius",
)

effective_area = OceanDataField(
    nc_name="effective_area",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="real",
    postgres_column_or_query_name="effective_area",
)


cost_association = OceanDataField(
    nc_name="cost_association",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="real",
    postgres_column_or_query_name="cost_association",
)

observation_flag = OceanDataField(
    nc_name="observation_flag",
    nc_scale=1,
    nc_offset=0,
    python_type=bool,
    postgres_type="boolean",
    postgres_column_or_query_name="observation_flag",
)

observation_number = OceanDataField(
    nc_name="observation_number",
    nc_scale=1,
    nc_offset=0,
    python_type=int,
    postgres_type="smallint",
    postgres_column_or_query_name="observation_number",
)

distance = OceanDataField(
    nc_name="distance",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="double precision",
    postgres_column_or_query_name="distance",
    custom_calculation="""
        ST_Distance(
            ST_MakePoint(%(longitude)s, %(latitude)s),
            eddy_point
        )
    """,
)

delta_t = OceanDataField(
    nc_name="delta_t",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="double precision",
    postgres_column_or_query_name="delta_t",
    custom_calculation="""
        EXTRACT(EPOCH FROM (%(central_date_time)s - date_time))
    """,
)
speed_average = OceanDataField(
    nc_name="speed_average",
    nc_scale=1,
    nc_offset=0,
    python_type=int,
    postgres_type="integer",
    postgres_column_or_query_name="speed_average",
)

speed_radius = OceanDataField(
    nc_name="speed_radius",
    nc_scale=1,
    nc_offset=0,
    python_type=int,
    postgres_type="smallint",
    postgres_column_or_query_name="speed_radius",
)

speed_area = OceanDataField(
    nc_name="speed_area",
    nc_scale=1,
    nc_offset=0,
    python_type=np.floating,
    postgres_type="real",
    postgres_column_or_query_name="speed_area",
)


eddy_schema = {
    "latitude": latitude,
    "longitude": longitude,
    "time": date_time,
    "track": track,
    "cyclonic_type": cyclonic_type,
    "amplitude": amplitude,
    "effective_radius": effective_radius,
    "effective_area": effective_area,
    "speed_average": speed_average,
    "speed_radius": speed_radius,
    "speed_area": speed_area,
    "cost_association": cost_association,
    "observation_flag": observation_flag,
    "observation_number": observation_number,
}



