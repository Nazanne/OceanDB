from typing import TypeVar, Any, Type
import psycopg as pg
from typing import Any, List, Dict, Optional

from OceanDB.OceanDB import OceanDB
from OceanDB.data_access.metadata import METADATA_REGISTRY
from OceanDB.ocean_data.ocean_data import OceanData, Dataset
from OceanDB.ocean_data.dataset import Dataset


D = TypeVar("D", bound=Dataset[Any])

class Query(OceanDB):

    METADATA = METADATA_REGISTRY

    def execute_query(self,
                        query: str,
                        params: dict[str, Any],
                        requested_fields: list[str],
                        dataset_cls: Type[D]
                      ) -> OceanData[D]:
        try:
            with pg.connect(self.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()

                    # dataset = dataset_cls.from_rows(
                    #     rows=rows,
                    #     name=
                    # )
                    return dataset

        except Exception as ex:
            self.logger.exception(f"Error while executing query {ex}")
            raise


class AlongTrackQuery(Query):
    missions = [
        "al", "alg", "c2", "c2n", "e1g", "e1", "e2", "en", "enn",
        "g2", "h2a", "h2b", "j1g", "j1", "j1n", "j2g", "j2", "j2n",
        "j3", "j3n", "s3a", "s3b", "s6a", "tp", "tpn",
    ]

    nearest_neighbor_query = "queries/along_track/geographic_nearest_neighbor.sql"
    along_track_spatiotemporal_query = "queries/along_track/geographic_points_in_spatialtemporal_window.sql"

    projected_spatio_temporal_query_mask = (
        "queries/along_track/geographic_points_in_spatialtemporal_projected_window_nomask.sql"
    )
    projected_spatio_temporal_query_no_mask = (
        "queries/along_track/geographic_points_in_spatialtemporal_window.sql"
    )

query = AlongTrackQuery()

query_string = query.load_sql_file("queries/along_track/geographic_points_in_spatialtemporal_window.sql")
radius = 500_000.0
missions = ['al']

basin_ids = query.basin_mask(latitudes, latitudes)
connected_basin_ids = list(map(self.basin_connection_map.get, basin_ids))

params = [
    {
        "longitude": lon,
        "latitude": lat,
        "distance": r,
        "central_date_time": dt,
        "time_delta": time_window,
        "connected_basin_ids": basins,
        "missions": [missions],
    }
    for lat, lon, dt, basins, r in zip(
        latitudes, longitudes, dates, connected_basin_ids, radii
    )
]

query.execute_query(

)