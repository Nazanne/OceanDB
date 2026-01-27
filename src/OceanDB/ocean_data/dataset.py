from typing import Mapping
from abc import ABC

from ..ocean_data.ocean_data import OceanDataField


class Dataset[K, T](Mapping[K, T], ABC):
    """
    Immutable, column-oriented dataset.

    Represents the result of a query, ingestion step, or transformation.
    """

    def __init__(
        self,
        *,
        name: str,
        data: Mapping[K, T],
        dtypes: Mapping[K, type],
    ):
        self.name = name
        self._data = dict(data)
        self._dtypes = dict(dtypes)

    def __getitem__(self, key: K) -> T:
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        # number of columns, not rows
        return len(self._data)

    def to_xarray(self):
        raise NotImplementedError()

    def to_netcdf(self):
        raise NotImplementedError()

    @classmethod
    def schema(cls) -> Mapping[K, OceanDataField]:
        """
        The schema for allowed input data for this dataset
        """
        raise NotImplementedError("Each dataset must define a schema")
