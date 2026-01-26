from typing import Generic, TypeVar, Union, Mapping, Any, Sequence
import numpy as np
import numpy.typing as npt
import netCDF4 as nc

from OceanDB.ocean_data.dataset import Dataset
from OceanDB.ocean_data.netcdf import write_dataset_to_group

D = TypeVar("D", bound=Dataset[Any])

FloatArray = npt.NDArray[np.floating]


class OceanData(Generic[D]):
    """
    Container for multiple datasets that together form
    one logical OceanDB query result.
    """

    def __init__(self):
        self._datasets: dict[str, D] = {}

    def add(self, dataset: D) -> None:
        if dataset.name in self._datasets:
            raise KeyError(f"Dataset '{dataset.name}' already exists")
        self._datasets[dataset.name] = dataset

    def get(self, name: str) -> D:
        return self._datasets[name]

    def datasets(self) -> Mapping[str, D]:
        return self._datasets

    def to_netcdf(self, path: str) -> None:
        """
        Write this OceanData object to a NetCDF file.

        Each contained Dataset becomes a NetCDF group with the dataset's name.
        """
        with nc.Dataset(path, "w", format="NETCDF4") as root:
            root.Conventions = "CF-1.9"
            root.source = "OceanDB"

            for name, dataset in self._datasets.items():
                grp = root.createGroup(name)
                write_dataset_to_group(grp, dataset, dim_name="obs")

    def __len__(self) -> int:
        return len(self._datasets)

    def __iter__(self):
        return iter(self._datasets)

    def __contains__(self, name: str) -> bool:
        return name in self._datasets

    def __getitem__(self, name: str) -> D:
        return self._datasets[name]

    def __repr__(self) -> str:
        if not self._datasets:
            return "<OceanData>\n  (no datasets)"

        lines = ["<OceanData>"]
        for name in self._datasets:
            lines.append(f"  └─ {name}")

        return "\n".join(lines)
