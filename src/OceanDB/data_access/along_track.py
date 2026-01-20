from datetime import datetime, timedelta
from typing import Iterable, List
import psycopg as pg
import numpy.typing as npt
import numpy as np

from OceanDB.data_access.base_query import BaseQuery
from OceanDB.ocean_data.datasets import AlongTrackDataset
from OceanDB.ocean_data.ocean_data import OceanData


class AlongTrack(BaseQuery):
    """
    Query/Service object

    Runs queries, returns datasets and bundles them into OceanData

    """

    # Domain key used by BaseQuery metadata registry
    ALONG_TRACK_DOMAIN = "along_track"

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



    def __init__(self):
        super().__init__()


    def _build_along_track_dataset(
            self,
            rows,
    ) -> OceanData[AlongTrackDataset]:
        ocean_data = OceanData()
        ocean_data.add(
            AlongTrackDataset.from_rows(
                rows,
                variable_scale_factor=self.METADATA['along_track'],
            )
        )
        return ocean_data


    def geographic_nearest_neighbors_dt(
            self,
            latitudes: npt.NDArray[np.floating],
            longitudes: npt.NDArray[np.floating],
            dates: List[datetime],
            time_window=timedelta(seconds=856710),
            missions=None,
    ) -> Iterable[OceanData[AlongTrackDataset] | None]:
        """
        Given an array of spatiotemporal points, returns the THREE closest data points to each
        """

        query = self.load_sql_file(self.nearest_neighbor_query)

        if missions is None:
            missions = self.missions

        basin_ids = self.basin_mask(latitudes, longitudes)
        connected_basin_ids = list(map(self.basin_connection_map.get, basin_ids))
        params = [
            {
                "latitude": latitude,
                "longitude": longitude,
                "central_date_time": date,
                "connected_basin_ids": connected_basin_ids,
                "time_delta": str(time_window / 2),
                "missions": missions,
            }
            for latitude, longitude, date, connected_basin_ids in zip(
                latitudes, longitudes, dates, connected_basin_ids
            )
        ]

        with pg.connect(self.config.postgres_dsn) as connection:
            with connection.cursor(row_factory=pg.rows.dict_row) as cursor:
                cursor.executemany(query, params, returning=True)
                while True:
                    rows = cursor.fetchall()
                    if not rows:
                        yield None
                    else:
                        data =  self._build_along_track_dataset(rows)
                        yield data
                    if not cursor.nextset():
                        break


    def geographic_points_in_r_dt(
            self,
            latitudes: npt.NDArray,
            longitudes: npt.NDArray,
            dates: List[datetime],
            radii: List[float] | float = 500_000.0,
            time_window: timedelta = timedelta(days=10),
            missions: list[str] | None = None,
    ) -> Iterable[OceanData[AlongTrackDataset] | None]:
        """
        Query along-track points within spatial + temporal windows.

        Yields one AlongTrackDataset per query point, or None if empty.
        """

        query = self.load_sql_file(self.along_track_spatiotemporal_query)

        if missions is None:
            missions = self.missions

        if not isinstance(radii, list):
            radii = [radii] * len(latitudes)

        basin_ids = self.basin_mask(latitudes, longitudes)
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

        with pg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor(row_factory=pg.rows.dict_row) as cur:
                cur.executemany(query, params, returning=True)

                while True:
                    rows = cur.fetchall()

                    if not rows:
                        yield None
                    else:
                        yield self._build_along_track_dataset(rows)

                    if not cur.nextset():
                        break
