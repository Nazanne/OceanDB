import numpy as np

from OceanDB.ocean_data.ocean_data import Dataset, OceanDataFactory
from OceanDB.ocean_data.schemas import AlongTrackRadiusFields


class AlongTrackDataset(Dataset[AlongTrackRadiusFields]):
    """
    Along-track altimetry points associated with an eddy.
    """

    name = "along_track"

    schema = {
        "latitude": np.float64,
        "longitude": np.float64,
        "sla_filtered": np.float64,
        "distance": np.float64,
        "delta_t": np.float64,
    }

    dimensions = {
        "obs": tuple(schema.keys())
    }

    @classmethod
    def from_rows(cls, rows, *, variable_scale_factor=None):
        return OceanDataFactory.from_rows(
            rows=rows,
            name=cls.name,
            fields=cls.schema,
            scale={
                "sla_filtered": variable_scale_factor.get("sla_filtered").get("scale")
            },
        )