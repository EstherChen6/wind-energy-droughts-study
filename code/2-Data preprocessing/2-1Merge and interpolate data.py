# （1）Merge data
import os
import pandas as pd


def sort_key(file_name):
    # Extract year
    year = int(file_name.split("\\")[-1].split('_')[2][:4])
    return year


# Create a new folder to store the merged CSV files
output_folder = r"E:\happy research\data\SSP126\ssp126_hebing"
os.makedirs(output_folder, exist_ok=True)
fs = r"E:\happy research\data\SSP126\csv_ssp126"
# Iterate through the files in 55 folders
folders = [os.path.join(fs, i) for i in os.listdir(fs)]
folders = sorted(folders, key=sort_key)
print(folders)
for folder in folders:
    # csv
    print(f"{folder} have successed")
    for file in os.listdir(folder):
        if file.endswith('.csv'):
            file_path = os.path.join(folder, file)
            if os.path.isfile(file_path):
                # Read csv file
                df = pd.read_csv(file_path)
                # Save the data to a new CSV file
                merged_file_path = os.path.join(output_folder, file)
                if os.path.exists(merged_file_path):
                    # If the merged CSV file already exists, append the data to the existing file
                    existing_df = pd.read_csv(merged_file_path)
                    merged_df = pd.concat([existing_df.iloc[:-1], df], ignore_index=True)
                    print(f"{merged_file_path} success")
                    merged_df.to_csv(merged_file_path, index=False)
                else:
                    # If the merged CSV file does not exist, save the data directly to a new file
                    df.to_csv(merged_file_path, index=False)

# （2）Interpolate
import numpy as np
from scipy.interpolate import UnivariateSpline
from scipy.interpolate import interp1d
import os
import csv
import matplotlib.pyplot as plt
import pandas as pd


def insert_data(in_folder, out_folder):
    start_time = pd.Timestamp('2015-01-01 00:30')
    end_time = pd.Timestamp('2101-01-01 00:30')
    time_series = pd.date_range(start=start_time, end=end_time, freq='h')
    filelist = os.listdir(in_folder)
    for file in filelist:
        if os.path.exists(os.path.join(out_folder, file)):
            continue
        data = pd.DataFrame()
        data["time"] = time_series
        filepath = os.path.join(in_folder, file)
        df = pd.read_csv(filepath)
        column_lon = df.columns[1]
        # # Location
        # time_column = df["time"]
        # df["time"] = pd.to_datetime(time_column)
        # x_time = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
        # x = [pd.Timestamp(i) for i in x_time]
        # x_index = [np.where(time_series == i)[0][0] for i in x]
        # print(x_index)

        # To optimize the memory usage
        # The value is calculated
        # length1 = len(df[f"{column_lon}"])
        # print(length1)
        x = np.arange(0, 3*251289-2)[np.arange(0, 3*251289-2) % 3 == 0]
        # print(len(x))
        y = list(df.iloc[:, 1])
        # print(len(y))
        new_x = np.arange(0, 3*251289-2)
        # Extrapolation or interpolation
        f1 = interp1d(x, y, kind='linear')
        # Extrapolation
        # f2 = UnivariateSpline(x, y, s=0)
        new_y = f1(new_x)
        # new_y2 = f2(new_x)
        # Assignment
        data[f"{column_lon}"] = new_y
        # print(y[10:20])
        # # print(new_y2[30:58])
        # print(new_y[30:58])
        # plt.plot(new_x[30:58], new_y2[30:58],"o",label="0-order spline")
        # plt.plot(new_x[30:58], new_y[30:58],"x",label="1-order spline")
        # plt.legend()
        # plt.show()
        # # Save
        out_file = os.path.join(out_folder, file)
        data.to_csv(out_file,index=False)
        print(f"{out_file} success")


in_folder = r"E:\happy research\data\SSP126\ssp126_hebing"
out_folder = r"E:\happy research\data\SSP126\ssp126_hebing_linear"
os.makedirs(out_folder, exist_ok=True)

insert_data(in_folder, out_folder)

