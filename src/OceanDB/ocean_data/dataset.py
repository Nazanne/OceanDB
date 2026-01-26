from typing import Mapping, TypeVar, Generic
import numpy as np
import numpy.typing as npt

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
