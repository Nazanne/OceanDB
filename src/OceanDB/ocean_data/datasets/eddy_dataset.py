import numpy as np

from OceanDB.ocean_data.ocean_data import Dataset, OceanDataFactory
from OceanDB.ocean_data.schemas import EddyTrackFields


class EddyDataset(Dataset[EddyTrackFields]):
    """
    Eddy track observations.
    """

    name = "eddy"

    schema = {
        "track": np.int32,
        "cyclonic_type": np.int8,
        "observation_number": np.int32,
        "latitude": np.float64,
        "longitude": np.float64,
        "speed_radius": np.float64,
        "amplitude": np.float64,
    }

    dimensions = {
        "obs": tuple(schema.keys())
    }

    @classmethod
    def from_rows(cls, rows, *, variable_scale_factor=None):
        """
        Build EddyDataset from SQL rows.

        `variable_scale_factor` is optional and mirrors the AlongTrackDataset API,
        even if unused for now.
        """
        scale = {}

        if variable_scale_factor:
            for key, meta in variable_scale_factor.items():
                if meta.get("scale") is not None:
                    scale[key] = meta["scale"]

        return OceanDataFactory.from_rows(
            rows=rows,
            name=cls.name,
            fields=cls.schema,
            scale=scale or None,
        )