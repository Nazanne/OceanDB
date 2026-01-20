from __future__ import annotations
import psycopg as pg

from OceanDB.data_access.base_query import BaseQuery
from OceanDB.ocean_data.datasets import EddyDataset, AlongTrackDataset
from OceanDB.ocean_data.ocean_data import OceanData


class Eddy(BaseQuery):
    """
    Query / service object for eddy-related data.

    All public query methods return OceanData containers.
    """

    eddy_table_name: str = "eddy"

    along_track_near_eddy_query = "queries/eddy/along_near_eddy.sql"
    eddy_with_id_query = "queries/eddy/eddy_from_track_id.sql"

    # Domain keys used by BaseQuery
    EDDY_DOMAIN = "eddy"
    ALONG_TRACK_DOMAIN = "along_track"


    def eddy_with_track_id(
        self,
        track_id: int,
    ) -> OceanData[EddyDataset] | None:
        """
        Retrieve all observations for a single eddy track.

        Returns
        -------
        OceanData[EddyDataset] | None
        """
        query = self.load_sql_file(self.eddy_with_id_query)
        values = {"track_id": track_id}

        with pg.connect(self.config.postgres_dsn) as conn:
            with conn.cursor(row_factory=pg.rows.dict_row) as cur:
                cur.execute(query, values)
                rows = cur.fetchall()

        if not rows:
            return None

        eddy_ds = self.build_dataset(
            dataset_cls=EddyDataset,
            rows=rows,
            domain=self.EDDY_DOMAIN,
        )

        return self.build_ocean_data(eddy_ds)

    #
    # def along_track_points_near_eddy(
    #     self,
    #     track_id: int,
    # ) -> OceanData[EddyDataset | AlongTrackDataset] | None:
    #     """
    #     Retrieve eddy observations and their associated along-track points.
    #
    #     Returns a single OceanData container with:
    #       - EddyDataset
    #       - AlongTrackDataset
    #     """
    #
    #     # --- Step 1: get eddy temporal + basin context ---------------------
    #
    #     eddy_extent_query = """
    #     SELECT
    #         MIN(date_time) AS min_date,
    #         MAX(date_time) AS max_date,
    #         array_agg(DISTINCT connected_id)
    #           || array_agg(DISTINCT basin.id) AS basin_ids
    #     FROM eddy
    #     LEFT JOIN basin
    #       ON ST_Intersects(basin.basin_geog, eddy.eddy_point)
    #     LEFT JOIN basin_connections
    #       ON basin_connections.basin_id = basin.id
    #     WHERE eddy.track * eddy.cyclonic_type = %(track_id)s
    #     GROUP BY track, cyclonic_type;
    #     """
    #
    #     with pg.connect(self.config.postgres_dsn) as conn:
    #         with conn.cursor(row_factory=pg.rows.dict_row) as cur:
    #             cur.execute(eddy_extent_query, {"track_id": track_id})
    #             ctx = cur.fetchone()
    #
    #     if ctx is None:
    #         return None
    #
    #     # --- Step 2: query eddy observations --------------------------------
    #
    #     eddy_query = self.load_sql_file(self.eddy_with_id_query)
    #
    #     with pg.connect(self.config.postgres_dsn) as conn:
    #         with conn.cursor(row_factory=pg.rows.dict_row) as cur:
    #             cur.execute(eddy_query, {"track_id": track_id})
    #             eddy_rows = cur.fetchall()
    #
    #     if not eddy_rows:
    #         return None
    #
    #     # --- Step 3: query along-track points --------------------------------
    #
    #     along_query = self.load_sql_file(self.along_track_near_eddy_query)
    #
    #     params = {
    #         "track_id": track_id,
    #         "min_date": ctx["min_date"],
    #         "max_date": ctx["max_date"],
    #         "connected_basin_ids": ctx["basin_ids"],
    #     }
    #
    #     with pg.connect(self.config.postgres_dsn) as conn:
    #         with conn.cursor(row_factory=pg.rows.dict_row) as cur:
    #             cur.execute(along_query, params)
    #             along_rows = cur.fetchall()
    #
    #     # --- Step 4: build datasets -----------------------------------------
    #
    #     eddy_ds = self.build_dataset(
    #         dataset_cls=EddyDataset,
    #         rows=eddy_rows,
    #         domain=self.EDDY_DOMAIN,
    #     )
    #
    #     ocean_data = OceanData[EddyDataset | AlongTrackDataset]()
    #     ocean_data.add(eddy_ds)
    #
    #     if along_rows:
    #         along_ds = self.build_dataset(
    #             dataset_cls=AlongTrackDataset,
    #             rows=along_rows,
    #             domain=self.ALONG_TRACK_DOMAIN,
    #         )
    #         ocean_data.add(along_ds)
    #
    #     return ocean_data
    #
    # # ---------------------------------------------------------------------
    #
    # def get_eddy_tracks_from_times(
    #     self,
    #     start_date: datetime,
    #     end_date: datetime,
    # ) -> list[int]:
    #     """
    #     Retrieve distinct eddy track identifiers observed within a given time range.
    #     """
    #
    #     query = """
    #     SELECT DISTINCT track
    #     FROM eddy
    #     WHERE date_time >= %(start_date)s
    #       AND date_time <  %(end_date)s
    #     ORDER BY track;
    #     """
    #
    #     params = {
    #         "start_date": start_date,
    #         "end_date": end_date,
    #     }
    #
    #     with pg.connect(self.config.postgres_dsn) as conn:
    #         with conn.cursor() as cur:
    #             cur.execute(query, params)
    #             return [row[0] for row in cur.fetchall()]