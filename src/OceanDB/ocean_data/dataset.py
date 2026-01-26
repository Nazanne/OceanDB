from typing import Mapping, TypeVar, Generic
from dataclasses import dataclass
import numpy as np
import numpy.typing as npt

from OceanDB.ocean_query import OceanDataField

F = TypeVar("F", bound=str)
FloatArray = npt.NDArray[np.floating]


class Dataset(Mapping[F, FloatArray]):
    """
    Immutable, column-oriented dataset.

    Represents the result of a query, ingestion step, or transformation.
    """

    def __init__(
        self,
        *,
        name: str,
        data: Mapping[F, FloatArray],
        dtypes: Mapping[F, type],
    ):
        self.name = name
        self._data = dict(data)
        self._dtypes = dict(dtypes)

    def __getitem__(self, key: F) -> FloatArray:
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        # number of columns, not rows
        return len(self._data)

    @property
    def n_rows(self) -> int:
        return len(next(iter(self._data.values())))


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

from OceanDB.OceanDB import OceanDB
from dataclasses import dataclass
from datetime import datetime


@dataclass
class OceanDataField:
    nc_name: str
    nc_scale: int
    nc_offset: int
    python_type: type
    postgres_type: str
    postgres_column_name: str
    postgres_table: str


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
        python_type=float,  # seconds
        postgres_type="double precision",
        postgres_column_name="delta_t",
        postgres_table=None,
    ),
}




# from typing import Generic, TypeVar, Union, Mapping, Any, Sequence, Type
# import numpy as np
# import numpy.typing as npt
#
# F = TypeVar("F", bound=str)
# D = TypeVar("D", bound="Dataset[Any]")
#
# FloatArray = npt.NDArray[np.floating]
#
# Row = Mapping[str, Any]
#
# class Dataset(Mapping[F, FloatArray]):
#     """
#     Immutable container for a single logical dataset (→ one NetCDF group).
#
#     - name      : NetCDF group name
#     - data      : field → ndarray
#     - dtypes    : field → numpy dtype
#     """
#
#     def __init__(
#         self,
#         *,
#         name: str,
#         data: dict[F, FloatArray],
#         dtypes: Mapping[F, npt.DTypeLike],
#     ):
#         self.name = name
#         self.data = data
#         self.dtypes = dtypes
#
#     def __getitem__(self, key: F) -> FloatArray:
#         return self.data[key]
#
#     def __iter__(self):
#         return iter(self.data)
#
#     def __len__(self) -> int:
#         return len(self.data)
#
#     @classmethod
#     def from_rows(
#         cls: Type[D],
#         *,
#         rows: Sequence[Row],
#         name: str,
#         fields: Mapping[F, npt.DTypeLike],
#         scale: Mapping[str, float] | None = None,
#         add_offset: Mapping[str, float] | None = None,
#         default_dtype: npt.DTypeLike = np.float64,
#     ) -> D:
#
#         if not rows:
#             raise ValueError("Cannot build Dataset from empty row sequence")
#
#         scale = scale or {}
#         add_offset = add_offset or {}
#
#         data: dict[F, FloatArray] = {}
#
#         missing = [f for f in fields if f not in rows[0]]
#         if missing:
#             raise KeyError(
#                 f"Requested fields not present in SQL rows: {missing}"
#             )
#
#         for field, dtype in fields.items():
#             key = str(field)
#             col = [row[key] for row in rows]
#
#             arr = np.asarray(
#                 col,
#                 dtype=dtype if dtype is not None else default_dtype,
#             )
#
#             if key in scale:
#                 arr = arr * scale[key]
#
#             if key in add_offset:
#                 arr = arr + add_offset[key]
#
#             data[field] = arr
#
#         return cls(
#             name=name,
#             data=data,
#             dtypes=fields,
#         )
