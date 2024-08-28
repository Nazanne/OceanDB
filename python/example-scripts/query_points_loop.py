from OceanDB.AlongTrack import AlongTrack
import numpy as np
import datetime
import time

atdb = AlongTrack(db_name='ocean')

date = datetime.datetime(year=2021, month=5, day=15, hour=3)
lon = np.arange(320.-180., 340.-180., 1.)
lat = np.arange(20., 40., 1.)
lons, lats = np.meshgrid(lon, lat)
missions = ['s3b','s6a-lr']

start = time.time()
for data in atdb.projected_geographic_points_in_spatialtemporal_windows(lats.reshape(-1), lons.reshape(-1), date, missions=missions):
    a = data["delta_x"]+data["delta_y"]
end = time.time()
print(f"Script end. Total time: {end - start}")
