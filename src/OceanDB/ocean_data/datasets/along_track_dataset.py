from OceanDB.ocean_data.dataset import Dataset
from OceanDB.ocean_data.ocean_data import OceanDataField
import numpy as np
import numpy.typing as npt
from dataclasses import dataclass
from datetime import datetime
FloatArray = npt.NDArray[np.floating]

@dataclass(frozen=True)
class AlongTrackDataset:
    """
    Domain-specific view over an along-track Dataset.

    Represents a sequence of altimetry observations along a satellite track.
    """
    dataset: Dataset[str]

    # -----------------
    # Core coordinates
    # -----------------
    @property
    def latitude(self) -> FloatArray:
        return self.dataset["latitude"]

    @property
    def longitude(self) -> FloatArray:
        return self.dataset["longitude"]

    @property
    def time(self):
        return self.dataset["date_time"]

    # -----------------
    # Altimetry signals
    # -----------------
    @property
    def sla(self) -> FloatArray:
        # choose filtered by default — domain decision
        return self.dataset["sla_filtered"]

    @property
    def sla_unfiltered(self) -> FloatArray:
        return self.dataset["sla_unfiltered"]

    # -----------------
    # Optional derived fields
    # -----------------
    @property
    def distance(self) -> FloatArray | None:
        return self.dataset._data.get("distance")

    @property
    def delta_t(self) -> FloatArray | None:
        return self.dataset._data.get("delta_t")

    # -----------------
    # Domain invariants
    # -----------------
    def __post_init__(self):
        required = {"latitude", "longitude", "date_time"}
        missing = required - set(self.dataset)
        if missing:
            raise ValueError(f"Missing required fields: {missing}")




"""
Authoritative storage schema for the along_track table.

Defines the complete set of fields that exist in persistent storage and
how each field maps across PostgreSQL, NetCDF, and Python, including
type information and scaling semantics.

This schema is used for ingestion, query construction, validation, and
export, but does not imply that all fields are present in every query
or exposed by every domain dataset.
"""


AlongTrackFields = {

    "latitude": OceanDataField(
        nc_name="latitude",
        nc_scale=1,
        nc_offset=0,
        python_type=float,
        postgres_type="double precision",
        postgres_column_name="latitude",
        postgres_table="along_track",
    ),
    "longitude": OceanDataField(
        nc_name="longitude",
        nc_scale=1,
        nc_offset=0,
        python_type=float,
        postgres_type="double precision",
        postgres_column_name="longitude",
        postgres_table="along_track",
    ),

    "date_time": OceanDataField(
        nc_name="time",
        nc_scale=1,
        nc_offset=0,
        python_type=datetime,
        postgres_type="timestamp",
        postgres_column_name="date_time",
        postgres_table="along_track",
    ),
    "file_name": OceanDataField(
        nc_name="file_name",
        nc_scale=1,
        nc_offset=0,
        python_type=str,
        postgres_type="text",
        postgres_column_name="file_name",
        postgres_table="along_track",
    ),
    "mission": OceanDataField(
        nc_name="mission",
        nc_scale=1,
        nc_offset=0,
        python_type=str,
        postgres_type="text",
        postgres_column_name="mission",
        postgres_table="along_track",
    ),
    "track": OceanDataField(
        nc_name="track",
        nc_scale=1,
        nc_offset=0,
        python_type=int,
        postgres_type="smallint",
        postgres_column_name="track",
        postgres_table="along_track",
    ),
    "cycle": OceanDataField(
        nc_name="cycle",
        nc_scale=1,
        nc_offset=0,
        python_type=int,
        postgres_type="smallint",
        postgres_column_name="cycle",
        postgres_table="along_track",
    ),
    "basin_id": OceanDataField(
        nc_name="basin_id",
        nc_scale=1,
        nc_offset=0,
        python_type=int,
        postgres_type="smallint",
        postgres_column_name="basin_id",
        postgres_table="along_track",
    ),

    "sla_unfiltered": OceanDataField(
        nc_name="sla_unfiltered",
        nc_scale=1000,   # meters → millimeters
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_name="sla_unfiltered",
        postgres_table="along_track",
    ),
    "sla_filtered": OceanDataField(
        nc_name="sla_filtered",
        nc_scale=1000,
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_name="sla_filtered",
        postgres_table="along_track",
    ),
    "dac": OceanDataField(
        nc_name="dac",
        nc_scale=1000,
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_name="dac",
        postgres_table="along_track",
    ),
    "ocean_tide": OceanDataField(
        nc_name="ocean_tide",
        nc_scale=1000,
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_name="ocean_tide",
        postgres_table="along_track",
    ),
    "internal_tide": OceanDataField(
        nc_name="internal_tide",
        nc_scale=1000,
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_name="internal_tide",
        postgres_table="along_track",
    ),
    "lwe": OceanDataField(
        nc_name="lwe",
        nc_scale=1000,
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_name="lwe",
        postgres_table="along_track",
    ),
    "mdt": OceanDataField(
        nc_name="mdt",
        nc_scale=1000,
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_name="mdt",
        postgres_table="along_track",
    ),
    "tpa_correction": OceanDataField(
        nc_name="tpa_correction",
        nc_scale=1000,
        nc_offset=0,
        python_type=float,
        postgres_type="smallint",
        postgres_column_name="tpa_correction",
        postgres_table="along_track",
    ),
}

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
AlongTrackSpatioTemporalProjection = {
    # base fields
    "latitude": AlongTrackFields["latitude"],
    "longitude": AlongTrackFields["longitude"],
    "sla_filtered": AlongTrackFields["sla_filtered"],
    "date_time": AlongTrackFields["date_time"],
    # derived fields
    "distance": OceanDataField(
        nc_name="distance",
        nc_scale=1,
        nc_offset=0,
        python_type=float,
        postgres_type="double precision",
        postgres_column_name="distance",
        postgres_table=None,
    ),
    "delta_t": OceanDataField(
        nc_name="delta_t",
        nc_scale=1,
        nc_offset=0,
        python_type=float,  #
        postgres_type="double precision",
        postgres_column_name="delta_t",
        postgres_table=None,
    ),
}

