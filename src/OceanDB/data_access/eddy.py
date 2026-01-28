from datetime import datetime, timedelta
from typing import Iterable, List, Literal, get_args
import psycopg as pg
import numpy.typing as npt
import numpy as np

from OceanDB.data_access.base_query import BaseQuery
from OceanDB.data_access.schema.along_track_schema import along_track_fields, along_track_schema
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.ocean_data.fields.eddy_fields import eddy_schema


class Eddy(BaseQuery):
    along_track_near_eddy_query = "queries/eddy/along_near_eddy.sql"
    eddy_with_id_query = "queries/eddy/eddy_from_track_id.sql"

    def __init__(self):
        super().__init__()

    def eddy_with_track_id(
        self,
        track_id: int,
    ) -> Dataset[along_track_fields, npt.NDArray[np.floating]]:
        """
        Retrieve all observations for a single eddy track.

        Returns
        -------
        OceanData[EddyDataset] | None
        """
        query = self.load_sql_file(self.eddy_with_id_query)
        params = {"track_id": track_id}

        return self.execute_query(query, eddy_schema, params)
