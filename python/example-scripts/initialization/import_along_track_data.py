# import sys; sys.path.extend(['/Users/jearly/Documents/ProjectRepositories/OceanDB/python'])
from OceanDB.AlongTrack import AlongTrack

# list of missions to add to database
missions = ['tp', 'j1', 'j2', 'j3', 's3a', 's3b', 's6a-lr']
missions = ['j3', 's3a', 's3b', 's6a-lr']
# missions = ['s6a-lr']
missions = ['j2g']
missions = ['al', 'alg', 'c2', 'c2n', 'e1g', 'e1', 'e2', 'en', 'enn', 'g2', 'h2a', 'h2b', 'j1g', 'j1', 'j1n', 'j2g', 'j2', 'j2n', 'j3', 'j3n', 's3a', 's3b', 's6a-lr', 'tp', 'tpn'] # full set
# importing all missions takes 6 hours, not counting index building.
# 1,790,015,440 along_track points
# Batch import ended. Total time: 15926.344366788864
# Building along_track filename index
# Finished. Total time: 2384.867588043213
# Building along_track time index
# Finished. Total time: 1072.8962049484253
# Building along_track basin index
# Finished. Total time: 840.1349990367889
# Building along_track mission index
# Finished. Total time: 844.5353701114655
# Building along_track geographic point index
# Finished. Total time: 9704.787243127823
# Building along_track geometric point index
# Finished. Total time: 2483.733836889267
# Building along_track (point, time) index
# Finished. Total time: 11317.651740312576
# Building along_track (point, time, mission, basin) index
# Building along_track (point, time, mission, basin) index
# Finished. Total time: 25640.473814964294
# Starting VACUUM ANALYZE...
# Finished. Total time: 528.2728838920593

atdb = AlongTrack(db_name="ocean")

# atdb.drop_database()
# atdb.drop_along_track_metadata_table()
# atdb.create_along_track_metadata_table()

# Load Along Track NetCDF files from an existing directory of files
# atdb.create_along_track_table_partitions('monthly') Create partitions is done in real time as data is loaded.
# Add a partition size parameter to the insert data from NetCDF function?
# atdb.insert_along_track_data_from_netcdf('/Users/briancurtis/Documents/Eddy/along_test_ncs')
# atdb.insert_along_track_data_from_netcdf('/Volumes/MoreStorage/along-track-data/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_j1-l3-duacs_PT1S_202112/2002/04')
# atdb.drop_table('along_track')
# atdb.create_along_track_table()
# atdb.truncate_along_track_metadata_table()
# atdb.insert_along_track_data_from_netcdf_with_tuples('/Users/jearly/Documents/Data/along-track-data/SEALEVEL_GLO_PHY_L3_MY_008_062/cmems_obs-sl_glo_phy-ssh_my_j1-l3-duacs_PT1S_202112/2002/04')

# atdb.truncate_along_track_table()
# atdb.truncate_along_track_metadata_table()
atdb.drop_along_track_indices()
atdb.insert_along_track_data_from_netcdf_with_tuples(missions)
atdb.create_along_track_indices()
