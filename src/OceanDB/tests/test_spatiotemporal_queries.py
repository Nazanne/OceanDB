import numpy as np
from datetime import datetime
from OceanDB.data_access.along_track import AlongTrack

along_track = AlongTrack()

"""
TEST single point spatiotemporal query
"""

latitude = -69
longitude = 28.1
date = datetime(year=2013, month=3, day=14, hour=23)

along_track_query_result_iterator = along_track.geographic_points_in_r_dt(
    latitudes=np.array([latitude]), longitudes=np.array([longitude]), dates=[date]
)

along_track_output_list = list(along_track_query_result_iterator)
result = along_track_output_list[0]
