from typing import Mapping

from ..ocean_data.ocean_data import OceanDataField


class Dataset[K:str, T](Mapping[K, T]):
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
        schema: Mapping[K, OceanDataField],
    ):
        self.name = name
        self._data = dict(data)
        self._dtypes = dict(dtypes)
        self.schema = schema

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
