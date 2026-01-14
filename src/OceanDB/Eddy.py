from OceanDB.OceanDB import OceanDB
import psycopg as pg
from psycopg import sql
from dataclasses import dataclass
from datetime import datetime
from typing import Tuple


@dataclass(frozen=True, slots=True)
class EddyTrackObservation:
    """
    Single observation along an eddy track.

    Represents one time step / contour measurement of a cyclonic or
    anticyclonic eddy derived from altimetry data.
    """

    track: int
    cyclonic_type: int
    date_time: datetime

    latitude: float
    longitude: float

    observation_number: int
    speed_radius: float
    amplitude: float

    @property
    def point(self) -> Tuple[float, float]:
        """Return (lat, lon) tuple."""
        return (self.latitude, self.longitude)

    @property
    def signed_track_id(self) -> int:
        """track * cyclonic_type, useful for grouping."""
        return self.track * self.cyclonic_type


def eddy_track_observation_from_row(row) -> EddyTrackObservation:
    return EddyTrackObservation(
        track=row["track"],
        cyclonic_type=row["cyclonic_type"],
        date_time=row["date_time"],
        latitude=row["latitude"],
        longitude=row["longitude"],
        observation_number=row["observation_number"],
        speed_radius=row["speed_radius"],
        amplitude=row["amplitude"],
    )


@dataclass(frozen=True, slots=True)
class AlongTrackEddyObservation:
    """
    Along-track altimetry observation associated with a specific eddy.

    Returned from a spatial + temporal join between the `eddy` and
    `along_track` tables.
    """

    # Satellite / file metadata
    file_name: str
    track: int
    cycle: int

    # Geolocation
    latitude: float
    longitude: float

    # Time
    time: datetime

    # Sea level anomaly
    sla_unfiltered: float
    sla_filtered: float

    # Corrections / components
    dac: float
    ocean_tide: float
    internal_tide: float
    lwe: float
    mdt: float
    tpa_correction: float

    @property
    def point(self) -> Tuple[float, float]:
        """Return (lat, lon) tuple."""
        return (self.latitude, self.longitude)

    @property
    def sla_anomaly(self) -> float:
        """Alias for filtered SLA."""
        return self.sla_filtered




class Eddy(OceanDB):
    eddy_table_name: str = 'eddy'
    eddy_metadata_table_name: str = 'eddy_metadata'
    eddies_file_path: str
    # eddy_variable_metadata: dict = dict()
    variable_scale_factor: dict = dict()
    variable_add_offset: dict = dict()

    along_track_near_eddy_query = 'queries/eddy/along_track_near_eddy.sql'
    eddy_with_id_query = 'queries/eddy/eddy_from_track_id.sql'

    speed_radius_scale_factor = 50


    def __init__(self):
        super().__init__()
        self.init_variable_metadata()

    def init_variable_metadata(self):
        self.eddy_variable_metadata = [
            {'var_name': 'amplitude',
             'comment': "Magnitude of the height difference between the extremum of SSH within the eddy and the SSH around the effective contour defining the eddy edge",
             'long_name': "Amplitude",
             'units': "m",
             'scale_factor': 0.0001,
             'add_offset': 0,
             'dtype': 'uint16'},
            {'var_name': 'cost_association',
             'comment': "Cost value to associate one eddy with the next observation",
             'long_name': "Cost association between two eddies",
             'dtype': 'float32'},
            {'var_name': 'effective_area',
             'comment': "Area enclosed by the effective contour in m^2",
             'long_name': "Effective area",
             'units': "m^2",
             'dtype': 'float32'},
            {'var_name': 'effective_contour_height',
             'comment': "SSH filtered height for effective contour",
             'long_name': "Effective Contour Height",
             'units': "m",
             'dtype': 'float32'},
            {'var_name': 'effective_contour_latitude',
             'axis': "X",
             'comment': "Latitudes of effective contour",
             'long_name': "Effective Contour Latitudes",
             'units': "degrees_east",
             'scale_factor': 0.01,
             'add_offset': 0},
            {'var_name': 'effective_contour_longitude',
             'axis': "X",
             'comment': "Longitudes of the effective contour",
             'long_name': "Effective Contour Longitudes",
             'units': "degrees_east",
             'scale_factor': 0.01,
             'add_offset': 180.},
            {'var_name': 'effective_contour_shape_error',
             'comment': "Error criterion between the effective contour and its best fit circle",
             'long_name': "Effective Contour Shape Error",
             'units': "%",
             'scale_factor': 0.5,
             'add_offset': 0,
             'dtype': 'uint8'},
            {'var_name': 'effective_radius',
             'comment': "Radius of the best fit circle corresponding to the effective contour",
             'long_name': "Effective Radius",
             'units': "m",
             'scale_factor': 50.,
             'add_offset': 0,
             'dtype': 'uint16'},
            {'var_name': 'inner_contour_height',
             'comment': "SSH filtered height for the smallest detected contour",
             'long_name': "Inner Contour Height",
             'units': "m",
             'dtype': 'float32'},
            {'var_name': 'latitude',
             'axis': "Y",
             'comment': "Latitude center of the best fit circle",
             'long_name': "Eddy Center Latitude",
             'standard_name': "latitude",
             'units': "degrees_north",
             'dtype': 'float32'},
            {'var_name': 'latitude_max',
             'axis': "Y",
             'comment': "Latitude of the inner contour",
             'long_name': "Latitude of the SSH maximum",
             'standard_name': "latitude",
             'units': "degrees_north",
             'dtype': 'float32'},
            {'var_name': 'longitude',
             'axis': "X",
             'comment': "Longitude center of the best fit circle",
             'long_name': "Eddy Center Longitude",
             'standard_name': "longitude",
             'units': "degrees_east",
             'dtype': 'float32'},
            {'var_name': 'longitude_max',
             'axis': "X",
             'comment': "Longitude of the inner contour",
             'long_name': "Longitude of the SSH maximum",
             'standard_name': "longitude",
             'units': "degrees_east",
             'dtype': 'float32'},
            {'var_name': 'num_contours',
             'comment': "Number of contours selected for this eddy",
             'long_name': "Number of contours",
             'dtype': 'uint16'},
            {'var_name': 'num_point_e',
             'description': "Number of points for effective contour before resampling",
             'long_name': "number of points for effective contour",
             'units': "ordinal",
             'dtype': 'uint16'},
            {'var_name': 'num_point_s',
             'description': "Number of points for speed contour before resampling",
             'long_name': "number of points for speed contour",
             'units': "ordinal",
             'dtype': 'uint16'},
            {'var_name': 'observation_flag',
             'comment': "Flag indicating if the value is interpolated between two observations or not (0: observed eddy, 1: interpolated eddy)",
             'long_name': "Virtual Eddy Position",
             'dtype': 'int8'},
            {'var_name': 'observation_number',
             'comment': "Observation sequence number, days starting at the eddy first detection",
             'long_name': "Eddy temporal index in a trajectory",
             'dtype': 'uint16'},
            {'var_name': 'speed_area',
             'comment': "Area enclosed by the speed contour in m^2",
             'long_name': "Speed area",
             'units': "m^2",
             'dtype': 'float32'},
            {'var_name': 'speed_average',
             'comment': "Average speed of the contour defining the radius scale speed_radius",
             'long_name': "Maximum circum-averaged Speed",
             'units': "m/s",
             'scale_factor': 0.0001,
             'add_offset': 0,
             'dtype': 'uint16'},
            {'var_name': 'speed_contour_height',
             'comment': "SSH filtered height for speed contour",
             'long_name': "Speed Contour Height",
             'units': "m",
             'dtype': 'float32'},
            {'var_name': 'speed_contour_latitude',
             'axis': "X",
             'comment': "Latitudes of speed contour",
             'long_name': "Speed Contour Latitudes",
             'units': "degrees_east",
             'scale_factor': 0.01,
             'add_offset': 0,
             'dtype': 'int16'},
            {'var_name': 'speed_contour_longitude',
             'axis': "X",
             'comment': "Longitudes of speed contour",
             'long_name': "Speed Contour Longitudes",
             'units': "degrees_east",
             'scale_factor': 0.01,
             'add_offset': 180.,
             'dtype': 'int16'},
            {'var_name': 'speed_contour_shape_error',
             'comment': "Error criterion between the speed contour and its best fit circle",
             'long_name': "Speed Contour Shape Error",
             'units': "%",
             'scale_factor': 0.5,
             'add_offset': 0,
             'dtype': 'uint8'},
            {'var_name': 'speed_radius',
             'comment': "Radius of the best fit circle corresponding to the contour of maximum circum-average speed",
             'long_name': "Speed Radius",
             'units': "m",
             'scale_factor': 50.,
             'add_offset': 0,
             'dtype': 'uint16'},
            {'var_name': 'time',
             'axis': "T",
             'calendar': "proleptic_gregorian",
             'comment': "Date of this observation",
             'long_name': "Time",
             'standard_name': "time",
             'units': "days since 1950-01-01 00:00:00",
             'scale_factor': 1.15740740740741e-05,
             'add_offset': 0},
            {'var_name': 'track',
             'comment': "Trajectory identification number",
             'long_name': "Trajectory number",
             'dtype': 'uint32'},
            {'var_name': 'uavg_profile',
             'comment': "Speed averaged values from the effective contour inwards to the smallest contour, evenly spaced points",
             'long_name': "Radial Speed Profile",
             'units': "m/s",
             'scale_factor': 0.0001,
             'add_offset': 0,
             'dtype': 'uint16'}]



    def along_track_points_near_eddy(self, track_id):
        """
        Retrieve along-track altimetry points spatially and temporally associated
        with a given eddy track.

        This method performs a two-stage query:

        1. It first determines the temporal extent of the specified eddy track
           (minimum and maximum `date_time`) and collects all basin identifiers
           associated with the eddy, including directly intersecting basins and
           their connected basins.

        2. It then queries the `along_track` table for altimetry observations that:
           - Occur within the eddy's lifetime (with an additional 1-day tolerance),
           - Lie within a distance threshold of the eddy center
             (`speed_radius * scale_factor * 2.0`),
           - Belong to one of the basins connected to the eddy.

        Parameters
        ----------
        track_id : int
            Signed eddy track identifier. The sign encodes cyclonic polarity and
            is matched against `eddy.track * eddy.cyclonic_type`.

        Returns
        -------
        list[tuple]
            A list of rows from the `along_track` table containing altimetry
            measurements near the eddy. Each row includes spatial coordinates,
            sea level anomaly values, timing information, and geophysical
            correction terms.

        Notes
        -----
        - Temporal filtering is based on `TIMESTAMP WITHOUT TIME ZONE` columns;
          all timestamps are assumed to be naive and expressed in a consistent
          reference time (typically UTC).
        - Spatial filtering uses PostGIS geography types and `ST_DWithin`, with
          distances interpreted in meters.
        - The spatial search radius is derived from the eddy `speed_radius` and
          scaled using `self.variable_scale_factor["speed_radius"]`.
        - Basin connectivity is resolved via the `basin_connections` table.
        - This method assumes the eddy track exists; no explicit guard is
          performed for empty result sets.


        """
        eddy_query = """SELECT MIN(date_time), MAX(date_time), array_agg(distinct connected_id) || array_agg(distinct basin.id)
                            FROM eddy 
                            LEFT JOIN basin ON ST_Intersects(basin.basin_geog, eddy.eddy_point)
                            LEFT JOIN basin_connections ON basin_connections.basin_id = basin.id
                            WHERE eddy.track * eddy.cyclonic_type=%(track_id)s
                            GROUP BY track, cyclonic_type;"""


        along_query = """SELECT atk.file_name, atk.track, atk.cycle, atk.latitude, atk.longitude, atk.sla_unfiltered, atk.sla_filtered, atk.date_time as time, atk.dac, atk.ocean_tide, atk.internal_tide, atk.lwe, atk.mdt, atk.tpa_correction
                       FROM eddy
                       INNER JOIN along_track atk ON atk.date_time BETWEEN eddy.date_time AND (eddy.date_time + interval '1 day')
    	               AND st_dwithin(atk.along_track_point, eddy.eddy_point, (eddy.speed_radius * {speed_radius_scale_factor} * 2.0)::double precision)
                       WHERE eddy.track * eddy.cyclonic_type=%(track_id)s
                       AND atk.date_time BETWEEN '{min_date}'::timestamp AND '{max_date}'::timestamp
                       AND basin_id = ANY( ARRAY[{connected_basin_ids}] );"""
        values = {"track_id": track_id}

        with pg.connect(self.config.postgres_dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(eddy_query, values)
                data = cursor.fetchall()

                values["min_date"] = data[0][0]
                values["max_date"] = data[0][1]


                print(data)

                along_query = along_query.format(
                    # speed_radius_scale_factor=self.variable_scale_factor["speed_radius"],
                    speed_radius_scale_factor=100,
                    min_date=data[0][0],
                    max_date=data[0][1],
                    connected_basin_ids=data[0][2])
                cursor.execute(along_query, values)
                data = cursor.fetchall()

        return data

    def get_eddy_tracks_from_times(self, start_date, end_date):
        """
        Retrieve distinct eddy track identifiers observed within a given time range.

        This method queries the `eddy` table and returns all unique `track` values
        for which at least one observation has a `date_time` between `start_date`
        and `end_date` (inclusive).

        Parameters
        ----------
        start_date : datetime.datetime
            Start of the time range (inclusive). Must be a **naive datetime**
            (i.e. `tzinfo is None`) and expressed in the same reference time
            used when writing to the database (typically UTC).
        end_date : datetime.datetime
            End of the time range (inclusive). Must be a **naive datetime**
            (i.e. `tzinfo is None`) and expressed in the same reference time
            used when writing to the database (typically UTC).

        Returns
        -------
        list[int]
            A sorted list of distinct eddy track numbers that have observations
            within the specified time range.

        Notes
        -----
        - The underlying database column is `TIMESTAMP WITHOUT TIME ZONE`,
          so timezone-aware datetimes are not supported.
        - Passing timezone-aware datetimes will raise an error in psycopg.
        - The query is parameterized to prevent SQL injection.
        - Results are ordered by `track` for deterministic output.

        Examples
        --------
        [1023, 1024, 1025]
        """
        query = """
        SELECT DISTINCT track
        FROM eddy
        WHERE date_time >= %(start_date)s
          AND date_time <  %(end_date)s
        ORDER BY track;
        """

        params = {
            "start_date": start_date,  # datetime.datetime
            "end_date": end_date,
        }

        with pg.connect(self.config.postgres_dsn) as connection:
            with connection.cursor() as cur:
                cur.execute(query, params)
                tracks = [row[0] for row in cur.fetchall()]
            return tracks

    def eddy_with_track_id(self, track_id) -> list[EddyTrackObservation]:
        """
        Query eddy table by track id
        """
        query = self.load_sql_file(self.eddy_with_id_query)
        values = {"track_id": track_id}

        with pg.connect(self.config.postgres_dsn) as connection:
            with connection.cursor(row_factory=pg.rows.dict_row) as cursor:
                cursor.execute(query, values)
                observations = [
                    EddyTrackObservation(**row)
                    for row in cursor.fetchall()
                ]
        return observations

eddy = Eddy()
#eddy.eddy_with_id_querytrack_id(track_id=4)

tracks = eddy.get_eddy_tracks_from_times(
    start_date=datetime(2013, 1, 1),
    end_date=datetime(2013, 4, 1)
)

print(tracks)
# print(f"number of tracks {len(tracks)}")

data = eddy.along_track_points_near_eddy(track_id=9844)



# print(len(data))
# print(type(data))