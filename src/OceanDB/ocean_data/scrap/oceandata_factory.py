from typing import Generic, TypeVar, Union, Mapping, Any, Sequence
import numpy as np
import numpy.typing as npt

from OceanDB.ocean_data.dataset import Dataset, F, FloatArray


Row = Mapping[str, Any]


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
