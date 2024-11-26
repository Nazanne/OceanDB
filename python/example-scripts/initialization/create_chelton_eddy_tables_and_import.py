from OceanDB.CheltonEddy import CheltonEddy

chelton_eddy_db = CheltonEddy(db_name='ocean')

# chelton_eddy_db.drop_database()

# Build Database
# chelton_eddy_db.create_database()

chelton_eddy_db.drop_chelton_eddy_indices()
chelton_eddy_db.drop_chelton_eddy_table()

chelton_eddy_db.create_chelton_eddy_table()
chelton_eddy_db.create_chelton_eddy_indices()

# Database build complete. Now load data

# # Load Eddy NetCDF files from an existing directory of files
# # Chelton Eddy NetCDF is a single file: "Eddy Trajectory DT 2.0 Jan 1 1993 to Mar 7 2020.nc"
chelton_eddy_db.insert_chelton_eddy_data_from_netcdf_with_tuples()

# chelton_eddy_db.create_chelton_eddy_indices()