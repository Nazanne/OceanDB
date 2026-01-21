from typing import Generic, TypeVar, Union, Mapping, Any, Sequence, Type
import numpy as np
import numpy.typing as npt

F = TypeVar("F", bound=str)
D = TypeVar("D", bound="Dataset[Any]")

FloatArray = npt.NDArray[np.floating]

Row = Mapping[str, Any]

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

    def __getitem__(self, key: F) -> FloatArray:
        return self.data[key]

    def __iter__(self):
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    @classmethod
    def from_rows(
        cls: Type[D],
        *,
        rows: Sequence[Row],
        name: str,
        fields: Mapping[F, npt.DTypeLike],
        scale: Mapping[str, float] | None = None,
        add_offset: Mapping[str, float] | None = None,
        default_dtype: npt.DTypeLike = np.float64,
    ) -> D:

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

            if key in scale:
                arr = arr * scale[key]

            if key in add_offset:
                arr = arr + add_offset[key]

            data[field] = arr

        return cls(
            name=name,
            data=data,
            dtypes=fields,
        )
