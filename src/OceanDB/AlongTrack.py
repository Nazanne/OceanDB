from typing import Generator, List, Callable
import psycopg as pg
from datetime import timedelta, datetime
import numpy as np
import numpy.typing as npt

from OceanDB.OceanDB import OceanDB
from OceanDB.utils.projections import spherical_transverse_mercator_to_latitude_longitude, latitude_longitude_to_spherical_transverse_mercator, latitude_longitude_bounds_for_transverse_mercator_box
from OceanDB.OceanData import CreateOceanData, OceanData


class AlongTrack(OceanDB):
    along_track_table_name: str = 'along_track'
    along_track_metadata_table_name: str = 'along_track_metadata'
    ocean_basin_table_name: str = 'basin'
    ocean_basins_connections_table_name: str = 'basin_connection'
    variable_scale_factor: dict = dict()
    variable_add_offset: dict = dict()
    missions = ['al', 'alg', 'c2', 'c2n', 'e1g', 'e1', 'e2', 'en', 'enn', 'g2', 'h2a', 'h2b', 'j1g', 'j1', 'j1n', 'j2g',
                'j2', 'j2n', 'j3', 'j3n', 's3a', 's3b', 's6a', 'tp', 'tpn']


    nearest_neighbor_query = 'queries/geographic_nearest_neighbor.sql'
    geo_spatiotemporal_query = 'queries/geographic_points_in_spatialtemporal_window.sql'
    projected_spatio_temporal_query_mask = 'queries/geographic_points_in_spatialtemporal_projected_window_nomask.sql'
    projected_spatio_temporal_query_no_mask = 'queries/geographic_points_in_spatialtemporal_window.sql'


    def __init__(self):
        super().__init__()
        aList = AlongTrack.along_track_variable_metadata()
        for metadata in aList:
            if 'scale_factor' in metadata:
                self.variable_scale_factor[metadata['var_name']] = metadata['scale_factor']
            if 'add_offset' in metadata:
                self.variable_add_offset[metadata['var_name']] = metadata['add_offset']

        self.sla_geographic_output = CreateOceanData()
        self.sla_geographic_output.register('latitude', 'deg')
        self.sla_geographic_output.register('longitude', 'deg')
        self.sla_geographic_output.register('sla_filtered', 'm')
        self.sla_geographic_output.register('distance', 'm')
        self.sla_geographic_output.register('delta_t', 'date')

        self.sla_projected_output = CreateOceanData()

    @staticmethod
    def along_track_variable_metadata():
        along_track_variable_metadata = [
            {'var_name': 'sla_unfiltered',
             'comment': 'The sea level anomaly is the sea surface height above mean sea surface height; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[dac]+[ocean_tide]+[internal_tide]-[lwe]; see the product user manual for details',
             'long_name': 'Sea level anomaly not-filtered not-subsampled with dac, ocean_tide and lwe correction applied',
             'scale_factor': 0.001,
             'standard_name': 'sea_surface_height_above_sea_level',
             'units': 'm',
             'dtype': 'int16'},
            {'var_name': 'sla_filtered',
             'comment': 'The sea level anomaly is the sea surface height above mean sea surface height; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[dac]+[ocean_tide]+[internal_tide]-[lwe]; see the product user manual for details',
             'long_name': 'Sea level anomaly filtered not-subsampled with dac, ocean_tide and lwe correction applied',
             'scale_factor': 0.001,
             'add_offset': 0.,
             '_FillValue': 32767,
             'standard_name': 'sea_surface_height_above_sea_level',
             'units': 'm',
             'dtype': 'int16'},
            {'var_name': 'dac',
             'comment': 'The sla in this file is already corrected for the dac; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[dac]; see the product user manual for details',
             'long_name': 'Dynamic Atmospheric Correction', 'scale_factor': 0.001, 'standard_name': None,
             'units': 'm',
             'dtype': 'int16'},
            {'var_name': 'time',
             'comment': '',
             'long_name': 'Time of measurement',
             'scale_factor': None,
             'standard_name': 'time',
             'units': 'days since 1950-01-01 00:00:00',
             'calendar': 'gregorian'},
            {'var_name': 'track',
             'comment': '',
             'long_name': 'Track in cycle the measurement belongs to',
             'scale_factor': None,
             'standard_name': None,
             'units': '1\n',
             'dtype': 'int16'},
            {'var_name': 'cycle',
             'comment': '',
             'long_name': 'Cycle the measurement belongs to',
             'scale_factor': None,
             'standard_name': None,
             'units': '1',
             'dtype': 'int16'},
            {'var_name': 'ocean_tide',
              'comment': 'The sla in this file is already corrected for the ocean_tide; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[ocean_tide]; see the product user manual for details',
              'long_name': 'Ocean tide model',
              'scale_factor': 0.001,
              'standard_name': None,
              'units': 'm',
             'dtype': 'int16'},
            {'var_name': 'internal_tide',
             'comment': 'The sla in this file is already corrected for the internal_tide; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]+[internal_tide]; see the product user manual for details',
             'long_name': 'Internal tide correction',
             'scale_factor': 0.001,
             'standard_name': None,
             'units': 'm',
             'dtype': 'int16'},
            {'var_name': 'lwe',
             'comment': 'The sla in this file is already corrected for the lwe; the uncorrected sla can be computed as follows: [uncorrected sla]=[sla from product]-[lwe]; see the product user manual for details',
             'long_name': 'Long wavelength error',
             'scale_factor': 0.001,
             'standard_name': None,
             'units': 'm',
             'dtype': 'int16'},
            {'var_name': 'mdt',
             'comment': 'The mean dynamic topography is the sea surface height above geoid; it is used to compute the absolute dynamic tyopography adt=sla+mdt',
             'long_name': 'Mean dynamic topography',
             'scale_factor': 0.001,
             'standard_name': 'sea_surface_height_above_geoid',
             'units': 'm',
             'dtype': 'int16'}]
        return along_track_variable_metadata

    def geographic_nearest_neighbors_dt(self,
                                     latitudes: npt.NDArray[np.floating],
                                     longitudes: npt.NDArray[np.floating],
                                     dates: List[datetime],
                                     time_window=timedelta(seconds=856710),
                                     missions=None
                                     ) -> Generator[OceanData|None, None, None]:
        """
        Given an array of spatiotemporal points, returns the THREE closest data points to each
        """

        query = self.load_sql_file(self.nearest_neighbor_query)

        if missions is None:
            missions = self.missions

        basin_ids = self.basin_mask(latitudes, longitudes)
        connected_basin_ids = list( map(self.basin_connection_map.get, basin_ids) )
        params = [
            {
                "latitude": latitude,
                "longitude": longitude,
                "central_date_time": date,
                "connected_basin_ids": connected_basin_ids,
                "time_delta": str(time_window / 2),
                "missions": missions
             }
            for latitude, longitude, date, connected_basin_ids in zip(latitudes, longitudes, dates, connected_basin_ids)]

        with pg.connect(self.config.postgres_dsn) as connection:
            with connection.cursor(row_factory=pg.rows.dict_row) as cursor:
                cursor.executemany(query, params, returning=True)
                while True:
                    rows = cursor.fetchall()
                    if not rows:
                        yield None
                    else:
                        yield self.sla_geographic_output(rows)
                    if not cursor.nextset():
                        break

    def geographic_points_in_r_dt(self,
                                  latitudes: npt.NDArray[np.floating],
                                  longitudes: npt.NDArray[np.floating],
                                  dates: List[datetime],
                                  distances: List[float]|float=500000.0,
                                  time_window=timedelta(seconds=856710),
                                  missions=None
                                  ) -> Generator[SLA_Geographic|None, None, None]:
        """
        Runs the geographic_points_in_spatialtemporal_window query for every point in the latitudes and longitudes arrays and dates list.

        Returns all along_track points within the geospatial window within distance

        :param latitudes: n-array
        :param longitudes: n-array
        :param dates: n-list
        :param distances

        """
        query = self.load_sql_file(self.geo_spatiotemporal_query)

        if missions is None:
            missions = self.missions

        if not isinstance(distances, list):
            distances = [distances]*len(latitudes)


        basin_ids = self.basin_mask(latitudes, longitudes)
        connected_basin_ids = list( map(self.basin_connection_map.get, basin_ids) )

        params = [
            {
                "longitude": longitude,
                "latitude": latitude,
                "distance": distance,
                "central_date_time": date,
                "time_delta": time_window,
                "connected_basin_ids": connected_basins,
                "missions": [missions]
            }
            for latitude, longitude, date, connected_basins, distance in zip(latitudes, longitudes, dates, connected_basin_ids, distances)
        ]


        with pg.connect(self.config.postgres_dsn) as connection:
            with connection.cursor(row_factory=pg.rows.dict_row) as cursor:
                cursor.executemany(query, params, returning=True)
                while True:
                    rows = cursor.fetchall()
                    if not rows:
                        yield None
                    else:
                        data = SLA_Geographic.from_rows(rows, self.variable_scale_factor["sla_filtered"])
                        yield data
                    if not cursor.nextset():
                        break

    def projected_points_in_r_dt(self,
                                 latitudes: npt.NDArray[np.floating],
                                 longitudes: npt.NDArray[np.floating],
                                 dates: List[datetime],
                                 distances: List[float]|float=500000.0,
                                 time_window=timedelta(seconds=856710),
                                 missions=None
                                 ) -> Generator[SLA_Projected | None, None, None]:
        """
        Get projected points around a reference point in a geographic radius and time interval
        """

        sla_geographic_data_points = self.geographic_points_in_r_dt(
            latitudes=latitudes,
            longitudes=longitudes,
            dates = dates,
            distances=distances,
            time_window=time_window,
            missions=missions
        )
        for lat,lon,geo_points in zip(latitudes,longitudes,sla_geographic_data_points):
            if geo_points is None:
                yield None
            else:
                yield SLA_Projected.from_sla_geographic(geo_points, latitude=lat, longitude=lon)

    def projected_points_in_dx_dy_dt(
            self,
            latitudes: npt.NDArray[np.floating],
            longitudes: npt.NDArray[np.floating],
            dates: List[datetime],
            Lx: float = 500000.,
            Ly: float = 500000.,
            time_window=timedelta(seconds=856710),
            missions: List[str]|None=None,
            should_basin_mask: bool = True
            ) -> Generator[SLA_Projected | None, None, None]:
        """
        Get projected points around a reference point in a box in projected coordinates, and time interval

        should_basin_mask: ->
        NO MASK -> if should_basin_mask = True, only return points in the basin or connected basin.

        Panama Example
        should_basin_mask = False ->  returns points on BOTH sides of the Panama,
        should_basin_mask = True -> Returns only data in connected basin

        """

        if missions is None:
            missions = self.missions

        if should_basin_mask:
            query = self.load_sql_file(self.projected_spatio_temporal_query_mask)
        else:
            query = self.load_sql_file(self.projected_spatio_temporal_query_mask)

        [x0s, y0s, minLats, minLons, maxLats, maxLons] = latitude_longitude_bounds_for_transverse_mercator_box(latitudes, longitudes, 2*Lx, 2*Ly)

        params = [
            {
                "longitude": longitude,
                "latitude": latitude,
                "xmin": xmin,
                "ymin": ymin,
                "xmax": xmax,
                "ymax": ymax,
                "central_date_time": date,
                "time_delta": time_window,
                "missions": [missions]
            }
            for latitude, longitude, date, xmin, ymin, xmax, ymax in zip(
                latitudes,
                longitudes,
                dates,
                minLats,
                minLons,
                maxLats,
                maxLons
            )
        ]

        with pg.connect(self.config.postgres_dsn) as connection:
            with connection.cursor() as cursor:
                cursor.executemany(query, params, returning=True)
                for x0,y0 in zip(x0s, y0s):
                    rows = cursor.fetchall()
                    if not rows:
                        yield None
                    else:
                        geo_data = SLA_Geographic.from_rows(rows, self.variable_scale_factor["sla_filtered"])
                        yield SLA_Projected.from_sla_geographic_filter_dx_dy(
                                geo_data, Lx, Ly, x0=x0, y0=y0
                    )
                    if not cursor.nextset():
                        break
