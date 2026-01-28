"""
Projection-Aware Dataset Architecture

"""

from datetime import datetime, timedelta
from typing import Iterable, List, Literal, get_args, Self
import psycopg as pg
import numpy.typing as npt
import numpy as np

from OceanDB.data_access.base_query import BaseQuery
from OceanDB.ocean_data.dataset import Dataset
import OceanDB.ocean_data.fields.fields as fields
from OceanDB.ocean_data.ocean_data import OceanDataField


class AlongTrack(BaseQuery):
    """
    Query/Service object

    Runs queries, returns datasets and bundles them into OceanData


    Query service for along-track altimetry data.

    Executes parameterized geospatial and spatiotemporal SQL queries and
    returns domain-level AlongTrackDataset objects instead of raw rows.
    """


    Mission = Literal[
        "al",
        "alg",
        "c2",
        "c2n",
        "e1g",
        "e1",
        "e2",
        "en",
        "enn",
        "g2",
        "h2a",
        "h2b",
        "j1g",
        "j1",
        "j1n",
        "j2g",
        "j2",
        "j2n",
        "j3",
        "j3n",
        "s3a",
        "s3b",
        "s6a",
        "tp",
        "tpn",
    ]
    all_missions = list(get_args(Mission))


    along_track_fields = Literal[
        "latitude",
        "longitude",
        "date_time",
        "file_name",
        "mission",
        "track",
        "cycle",
        "basin_id",
        "sla_unfiltered",
        "sla_filtered",
        "dac",
        "ocean_tide",
        "internal_tide",
        "lwe",
        "mdt",
        "tpa_correction",
        "distance",
        "delta_t",
    ]

    schema : dict[along_track_fields, OceanDataField] = {
        "latitude": fields.latitude,
        "longitude": fields.longitude,
        "date_time": fields.date_time,
        "file_name": fields.file_name,
        "mission": fields.mission,
        "track": fields.track,
        "cycle": fields.cycle,
        "basin_id": fields.basin_id,
        "sla_unfiltered": fields.sla_unfiltered,
        "sla_filtered": fields.sla_filtered,
        "dac": fields.dac,
        "ocean_tide": fields.ocean_tide,
        "internal_tide": fields.internal_tide,
        "lwe": fields.lwe,
        "mdt": fields.mdt,
        "tpa_correction": fields.tpa_correction,
        "distance": fields.distance,
        "delta_t": fields.delta_t,
        }





    # Domain key used by BaseQuery metadata registry
    # ALONG_TRACK_DOMAIN = "along_track"

    nearest_neighbor_query = "queries/along_track/geographic_nearest_neighbor.sql"
    along_track_spatiotemporal_query = (
        "queries/along_track/geographic_points_in_spatialtemporal_window.sql"
    )

    projected_spatio_temporal_query_mask = "queries/along_track/geographic_points_in_spatialtemporal_projected_window_nomask.sql"
    projected_spatio_temporal_query_no_mask = (
        "queries/along_track/geographic_points_in_spatialtemporal_window.sql"
    )

    def __init__(self):
        super().__init__()

    def geographic_points_in_r_dt(
        self,
        latitudes: npt.NDArray,
        longitudes: npt.NDArray,
        dates: List[datetime],
        fields: list[along_track_fields],
        radii: List[float] | float = 500_000.0,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] = all_missions,
    ) -> Iterable[Dataset[along_track_fields, npt.NDArray[np.floating]] | None]:
        """
        Query along-track points within spatial + temporal windows.

        Yields one AlongTrackDataset per query point, or None if empty.
        """

        query_string = self.load_sql_file(self.along_track_spatiotemporal_query)
        query = pg.sql.SQL(query_string).format(
            fields=pg.sql.SQL(', ').join([
                self.schema[field].to_sql_query() for field in fields
        ]))


        if not isinstance(radii, list):
            radii = [float(radii)] * len(latitudes)

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
                "missions": missions,
            }
            for lat, lon, dt, basins, r in zip(
                latitudes, longitudes, dates, connected_basin_ids, radii
            )
        ]
        return self.do_query(query, self.schema, params)

    def geographic_nearest_neighbors_dt(
        self,
        latitudes: npt.NDArray[np.floating],
        longitudes: npt.NDArray[np.floating],
        dates: List[datetime],
        time_window=timedelta(seconds=856710),
        missions: list[Mission] = all_missions,
    ):
        # ) -> Iterable[OceanData[AlongTrackDataset] | None]:
        """
        Given an array of spatiotemporal points, returns the THREE closest data points to each
        """

        query = self.load_sql_file(self.nearest_neighbor_query)

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
                        along_track_ds = self.build_dataset(
                            dataset_cls=AlongTrackDataset,
                            rows=rows,
                            schema=AlongTrackSpatioTemporalProjection,
                        )
                        yield along_track_ds
                    if not cursor.nextset():
                        break
