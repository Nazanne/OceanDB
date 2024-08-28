from OceanDB.AlongTrack import AlongTrack
import numpy as np
import matplotlib.pyplot as plt
import datetime
import time

def gaussian_kernel(x2, sigma):
    return np.exp(-0.5 * x2 / (sigma*sigma)) / (sigma * np.sqrt(2 * np.pi))

def gaussian_kernel_smoother(x2, data, kernel_width):
    weights = gaussian_kernel(x2, kernel_width)

    # Normalize weights
    weights /= weights.sum()

    # Apply the weights to the data
    smoothed_data = np.sum(weights * data)
    return smoothed_data

atdb = AlongTrack()

date = datetime.datetime(year=2021, month=5, day=15, hour=3)
lon = np.arange(320.-180., 340.-180., 1.)
lat = np.arange(20., 40., 1.)
lons, lats = np.meshgrid(lon, lat)
latitudes = lats.reshape(-1)
longitudes = lons.reshape(-1)
smoothed_data = np.zeros_like(longitudes)
missions = ['s3b','s6a-lr']



start = time.time()
i = 0
for data in atdb.projected_geographic_points_in_spatialtemporal_windows(latitudes, longitudes, date, missions=missions):
    L = (50e3 + 250e3 * (900 / (latitudes[i] ** 2 + 900))) / 3.34
    out_of_bounds = data["sla_filtered"] > 1.0
    data["delta_x"] = data["delta_x"][~out_of_bounds]
    data["delta_y"] = data["delta_y"][~out_of_bounds]
    data["sla_filtered"] = data["sla_filtered"][~out_of_bounds]
    x2 = data["delta_x"]**2 + data["delta_y"]**2
    smoothed_data[i] = gaussian_kernel_smoother(x2, data["sla_filtered"], L)
    i = i + 1
end = time.time()
print(f"Script end. Total time: {end - start}")

plt.figure()
plt.scatter(longitudes, latitudes, c=smoothed_data, vmin=-0.25, vmax=0.25)
plt.show()