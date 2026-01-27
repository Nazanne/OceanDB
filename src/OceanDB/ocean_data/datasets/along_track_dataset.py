from OceanDB.ocean_data.dataset import Dataset
from OceanDB.ocean_data.ocean_data import OceanDataField
import numpy as np
import numpy.typing as npt
from typing import Literal, get_args
from datetime import datetime


class SpatiotemporalSchema:
    """
    Schema for arbitrary spatiotemporal data, indexed by latitude, longitude and time.
    """
    fields = Literal[
        "latitude",
        "longitude",
        "date_time",
    ]|None

    def dict(self):
        return {field: self.__getattribute__(field) for group in get_args(self.fields) for field in get_args(group)}

    latitude = OceanDataField(
        nc_name="latitude",
        nc_scale=1,
        nc_offset=0,
        python_type=float,
        postgres_type="double precision",
        postgres_column_or_query_name="latitude",
    )

    longitude = OceanDataField(
        nc_name="longitude",
        nc_scale=1,
        nc_offset=0,
        python_type=float,
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


class AlongTrackSchema(SpatiotemporalSchema):
    """
    Authoritative storage schema for the along_track table.

    Defines the complete set of fields that exist in persistent storage and
    how each field maps across PostgreSQL, NetCDF, and Python, including
    type information and scaling semantics.

    This schema is used for ingestion, query construction, validation, and
    export, but does not imply that all fields are present in every query
    or exposed by every domain dataset.
    """
    fields = SpatiotemporalSchema.fields|Literal[
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
        "tpa_correction"
    ]


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
        python_type=float,
        postgres_type="smallint",
        postgres_column_or_query_name="sla_unfiltered",
    )
    sla_filtered = OceanDataField(
        nc_name="sla_filtered",
        nc_scale=1000,
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_or_query_name="sla_filtered",
    )
    dac = OceanDataField(
        nc_name="dac",
        nc_scale=1000,
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_or_query_name="dac",
    )
    ocean_tide = OceanDataField(
        nc_name="ocean_tide",
        nc_scale=1000,
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_or_query_name="ocean_tide",
    )
    internal_tide = OceanDataField(
        nc_name="internal_tide",
        nc_scale=1000,
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_or_query_name="internal_tide",
    )
    lwe = OceanDataField(
        nc_name="lwe",
        nc_scale=1000,
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_or_query_name="lwe",
    )
    mdt = OceanDataField(
        nc_name="mdt",
        nc_scale=1000,
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_or_query_name="mdt",
    )
    tpa_correction = OceanDataField(
        nc_name="tpa_correction",
        nc_scale=1000,
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_or_query_name="tpa_correction",
    )


class AlongTrackSpatioTemporalProjectionSchema(AlongTrackSchema):
    """
    Projection schema for spatiotemporal along-track queries.

    Specifies the exact set of base and derived fields returned by a
    spatiotemporal query, including computed quantities such as distance
    and temporal offset.

    This projection defines data availability and typing for a specific
    query, and is used to construct aligned runtime Datasets. It is
    intentionally separate from both the full storage schema and the
    domain dataset semantics.
    """
    fields = AlongTrackSchema.fields | Literal[
        "distance",
        "delta_t",
    ]

    distance = OceanDataField(
        nc_name="distance",
        nc_scale=1,
        nc_offset=0,
        python_type=float,
        postgres_type="double precision",
        postgres_column_or_query_name="distance",
    )
    delta_t = OceanDataField(
        nc_name="delta_t",
        nc_scale=1,
        nc_offset=0,
        python_type=float,  #
        postgres_type="double precision",
        postgres_column_or_query_name="delta_t",
    )
