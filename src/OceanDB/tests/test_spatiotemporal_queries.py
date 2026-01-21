import numpy as np
from datetime import datetime
from OceanDB.data_access.along_track import AlongTrack
from OceanDB.ocean_data.datasets import AlongTrackDataset
from OceanDB.ocean_data.ocean_data import OceanData

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
along_track_ocean_data: OceanData[AlongTrackDataset] = along_track_output_list[0]


along_track_data = along_track_ocean_data['along_track']






nearest_neighbor_ocean_data = along_track.geographic_nearest_neighbors_dt(
    latitudes=np.array([latitude]),
    longitudes=np.array([longitude]),
    dates=[date],
    missions=["al"],
)
nn_ocean_data_list = list(nearest_neighbor_ocean_data)
nearest_neighbor_along_track_data = nn_ocean_data_list[0]
nn_data = nearest_neighbor_along_track_data['along_track']
