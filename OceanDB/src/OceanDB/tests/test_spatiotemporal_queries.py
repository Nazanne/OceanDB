from datetime import datetime

from OceanDB.AlongTrack import AlongTrack

along_track = AlongTrack()


"""
TEST single point spatiotemporal query
"""

latitude = -69
longitude = 28
datetime(year=2013, month=3, day=14, hour=5)

#
# sla_geographic = along_track.geographic_points_in_spatialtemporal_window(
#     latitude=latitude,
#     longitude=longitude,
#     date= datetime(year=2013, month=3, day=14, hour=5),
#     missions=['al']
# )
#
# str(sla_geographic)


"""
TEST multipoint spatiotemporal query
"""

latitudes = [-69, -60]
longitudes = [ 28, 28]
dates = [
    datetime(year=2013, month=3, day=14, hour=5),
    datetime(year=2013, month=3, day=14, hour=5)
]

along_track.geographic_points_in_spatialtemporal_windows(
    latitudes=latitudes,
    longitudes=longitudes
)