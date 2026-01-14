from typing import Any

import numpy as np
import numpy.typing as npt


class OceanData:
    def __init__(
        self,
        data: list[dict[str, Any]],
        columns: dict[str, str],
    ):
        self.data = data
        self.columns = columns

    def to_numpy(self) -> dict[str, npt.NDArray]:
        return {column: np.array([row[column] for row in self.data]) for column in self.columns}

    def to_netcdf(self): ...
    
    def to_xarray(self): ...


class CreateOceanData:
    def __init__(self):
        self.columns = {}

    def register(self, column_name: str, units: str):
        self.columns[column_name] = units

    def __call__(self, data: list[dict[str, Any]]) -> OceanData:
        return OceanData(data, self.columns)

