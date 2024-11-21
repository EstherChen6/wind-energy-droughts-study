# Read wind speed data for each grid-cell with latitude and longitude.
import os
import xarray as xr
import numpy as np
import pandas as pd
import csv
import shutil



# Get all files in the folder
nc_folder = r"E:\happy research\data\nc\nc_ssp370"
nc_files = os.listdir(nc_folder)


j = 0
# Read each of the files
for nc_file in nc_files:
    print(j, nc_file)
    j += 1
    # Open file
    ds = xr.open_dataset(os.path.join(nc_folder, nc_file))

    csv_counter = 1  # Counter to follow CSV file

    # Get latitude and longitude
    with open(r"E:\happy research\geo\geo_result\geo3.csv", 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row
        m = 0
        for row in reader:
            lon = float(row[2])  # Assuming the latitude is in the first column
            lat = float(row[3])  # Assuming the longitude is in the second column

            find_latindex = lat
            find_lonindex = lon
            sfcWind_tem = ds['sfcWind'].values
            lon_vals = ds['lon'].values
            lat_vals = ds['lat'].values

            lat_index = np.abs(lat_vals - find_latindex).argmin()
            lon_index = np.abs(lon_vals - find_lonindex).argmin()

            sfcWind_select = sfcWind_tem[:, lat_index, lon_index]
            time = ds['time'].values
            df = pd.DataFrame({'time': time, str(lat)+'_'+str(lon): sfcWind_select})

            # Create corresponding folders
            a, b, c, d, e, f, g = nc_file.split('_')
            g = g.replace('.nc', '')
            folder_path = os.path.join(r"E:\happy research\data\csv_ssp370", f"csv_{d}_{g}")
            os.makedirs(folder_path, exist_ok=True)

            # Dynamically create CSV file names
            csv_filename = os.path.join(folder_path, f"{csv_counter}_{lon}_{lat}.csv")

            # Write data into a CSV file
            df.to_csv(csv_filename, index=False)

            print(f"{m}  {row} ok")
            csv_counter += 1
            m += 1