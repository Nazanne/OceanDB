filename = '../python/example-scripts/sla_mapped_2021-05-15_03-00_res_1_4.nc';
% filename = 'eddy_-41.nc';

gk_lat = ncread(filename,'gaussian_kernel/latitude');
gk_lon = ncread(filename,'gaussian_kernel/longitude');
gk_amp = ncread(filename,'gaussian_kernel/sla');

nn_lat = ncread(filename,'nearest_neighbor/latitude');
nn_lon = ncread(filename,'nearest_neighbor/longitude');
nn_amp = ncread(filename,'nearest_neighbor/sla');

figure(Position=[0 0 1000 850])
tiledlayout(2,1,TileSpacing="tight")
nexttile
pcolor(nn_lon, nn_lat, nn_amp.'), shading flat, clim([-0.5 0.5])
axis equal, xlim([min(gk_lon) max(gk_lon)]), ylim([min(gk_lat) max(gk_lat)])
title('Nearest neighbor')

nexttile
pcolor(gk_lon, gk_lat, gk_amp.'), shading flat, clim([-0.5 0.5])
axis equal, xlim([min(gk_lon) max(gk_lon)]), ylim([min(gk_lat) max(gk_lat)])
colormap(cmocean('balance'))
title('Gaussian kernel')

print('sla_comparison_1_4.png', '-dpng', '-r150')