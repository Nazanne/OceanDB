from __future__ import annotations

from typing import Any, Mapping, Sequence, TypeVar
import numpy as np
import numpy.typing as npt
import netCDF4 as nc

from OceanDB.ocean_data.netcdf import write_dataset_to_group

F = TypeVar("F", bound=str)

FloatArray = npt.NDArray[np.floating]
Row = Mapping[str, Any]

class OceanData:
    """
    Container for multiple datasets that together form
    one logical OceanDB query result.
    """

    def __init__(self):
        self._datasets: dict[str, Any] = {}

    def add(self, dataset) -> None:
        if dataset.name in self._datasets:
            raise KeyError(f"Dataset '{dataset.name}' already exists")

        self._datasets[dataset.name] = dataset

    def get(self, name: str):
        return self._datasets[name]

    def datasets(self):
        return self._datasets.values()

    def to_netcdf(self, path: str) -> None:
        """
        Write this OceanData object to a NetCDF file.

        Each contained Dataset becomes a NetCDF group with the dataset's name.
        """
        with nc.Dataset(path, "w", format="NETCDF4") as root:
            # Optional: global attrs (nice for provenance)
            root.Conventions = "CF-1.9"
            root.source = "OceanDB"

            for name, dataset in self._datasets.items():
                grp = root.createGroup(name)
                write_dataset_to_group(grp, dataset, dim_name="obs")


    def __len__(self) -> int:
        """Number of datasets contained."""
        return len(self._datasets)

    def __iter__(self):
        """Iterate over dataset names."""
        return iter(self._datasets)

    def __contains__(self, name: str) -> bool:
        """`'along_track' in ocean_data`"""
        return name in self._datasets

    def __getitem__(self, name: str):
        """`ocean_data['along_track']`"""
        return self._datasets[name]

    def __repr__(self) -> str:
        if not self._datasets:
            return "<OceanData>\n  (no datasets)"

        lines = ["<OceanData>"]
        for name in self._datasets:
            lines.append(f"  └─ {name}")

        return "\n".join(lines)



class Dataset(Mapping[F, FloatArray]):
    """
    Immutable container for a single logical dataset (→ one NetCDF group).

    - name      : NetCDF group name
    - data      : field → ndarray
    - dtypes    : field → numpy dtype
    """

    def __init__(
        self,
        *,
        name: str,
        data: dict[F, FloatArray],
        dtypes: Mapping[F, npt.DTypeLike],
    ):
        self.name = name
        self.data = data
        self.dtypes = dtypes

    # Mapping interface (nice ergonomics)
    def __getitem__(self, key: F) -> FloatArray:
        return self.data[key]

    def __iter__(self):
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)


class OceanDataFactory:
    """
    Build Dataset objects from Postgres rows.

    Responsibilities:
    - column extraction
    - dtype coercion
    - scaling / offset application
    - validation

    Non-responsibilities:
    - NetCDF
    - file IO
    - grouping / orchestration
    """

    @staticmethod
    def from_rows(
        *,
        rows: Sequence[Row],
        name: str,
        fields: Mapping[F, npt.DTypeLike],
        scale: Mapping[str, float] | None = None,
        add_offset: Mapping[str, float] | None = None,
        default_dtype: npt.DTypeLike = np.float64,
    ) -> Dataset[F]:

        if not rows:
            raise ValueError("Cannot build Dataset from empty row sequence")

        scale = scale or {}
        add_offset = add_offset or {}

        data: dict[F, FloatArray] = {}

        missing = [f for f in fields if f not in rows[0]]
        if missing:
            raise KeyError(
                f"Requested fields not present in SQL rows: {missing}"
            )

        for field, dtype in fields.items():
            key = str(field)

            col = [row[key] for row in rows]

            arr = np.asarray(
                col,
                dtype=dtype if dtype is not None else default_dtype,
            )

            s = scale.get(key)
            if s is not None:
                arr = arr * s

            o = add_offset.get(key)
            if o is not None:
                arr = arr + o

            data[field] = arr

        return Dataset(
            name=name,
            data=data,
            dtypes=fields,
        )
