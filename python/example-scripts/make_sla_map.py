from OceanDB.AlongTrack import AlongTrack
import numpy as np
import datetime
import time
import xarray as xr
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt

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
resolution = 0.5
lon_dim = np.arange(-120., -70., resolution) + resolution
lat_dim = np.arange(-10., 40., resolution) + resolution
lon_grid, lat_grid = np.meshgrid(lon_dim, lat_dim)
lat_world = lat_grid.reshape(-1)
lon_world = lon_grid.reshape(-1)
sla_world = np.empty_like(lon_world)
sla_world[:] = np.nan

basin_mask = atdb.basin_mask(lat_world, lon_world)
ocean_indices = (basin_mask > 0) & (basin_mask < 1000)
lat_ocean = lat_world[ocean_indices]
lon_ocean = lon_world[ocean_indices]
sla_ocean = sla_world[ocean_indices]

L = (50e3 + 250e3 * (900 / (lat_ocean ** 2 + 900))) / 3.34
radius = L * np.sqrt(-np.log(0.001))

missions = None #['s3b','s6a']

start = time.time()
i = 0

# radius = sqrt(-ln(0.001))*L will be sufficient
for data in atdb.projected_geographic_points_in_spatialtemporal_windows(lat_ocean, lon_ocean, date, distance=radius, missions=missions):

    # out_of_bounds = data["sla_filtered"] > 1.0
    # data["delta_x"] = data["delta_x"][~out_of_bounds]
    # data["delta_y"] = data["delta_y"][~out_of_bounds]
    # data["sla_filtered"] = data["sla_filtered"][~out_of_bounds]
    x2 = data["delta_x"]**2 + data["delta_y"]**2
    sla_ocean[i] = gaussian_kernel_smoother(x2, data["sla_filtered"], L[i])
    i = i + 1
end = time.time()
print(f"Script end. Total time: {end - start}")

plt.figure()
plt.scatter(lon_ocean, lat_ocean, c=sla_ocean,  vmin=-0.5, vmax=0.5)
plt.show()

sla_world[ocean_indices] = sla_ocean
sla_world = sla_world.reshape(lon_grid.shape)

sla_map = xr.DataArray(sla_world, coords={'latitude': lat_dim, 'longitude': lon_dim},
                                  dims=["latitude", "longitude"])
sla_map.to_netcdf("bob.nc", format="NETCDF4")

vmin = -.5
vmax = .5
norm = mpl.colors.Normalize(vmin=vmin,vmax=vmax)

plt.figure()
# ax = sla_map.plot.contourf(levels=100, norm=norm, cmap='RdBu_r')
ax = sla_map.plot.pcolormesh(norm=norm, cmap='RdBu_r')
_ = plt.title('Mapped Sea Level Anomaly')
plt.show()

print("done")