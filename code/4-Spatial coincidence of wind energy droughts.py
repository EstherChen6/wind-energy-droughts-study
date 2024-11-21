import os
import pandas as pd
import time
from datetime import datetime
import numpy as np
import csv

'''
1735*1735 Spatial correlation between any grids, with distance on the horizontal axis and spatial correlation on the vertical axis
One data for each ssp in each year, 86year*4 ssp + 86year spatial association matrix
Whether climate change causes simultaneous extreme events
Spatial correlation degree

Draw 4 plots, horizontal axis relative distance. The vertical axis correlation, scatter plot, represents the array, and the vertical axis represents the magnitude of the Jaccard coefficient
eg ssp585 is more correlated when it is closer and less correlated when it is farther away

One data for each ssp in each year, 86year*4 ssp + 86year spatial association matrix
Only climate scenarios, regardless of year

Data on historical scenarios
' ' '

' ' '
Calculate the spatial distance and put it in a dict, (lon1, lat1, lon2, lat2) : distance
Putting in relevant data
Compute a single jaccard
Calculate the overall Jaccard coefficient
Calculate all time and space
Look for points with anomalous Jaccard coefficients
'''


def distance_find(data_dict):
    # Read from CSV file
    csv_file = 'data.csv'
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read the first line as the key
        values = next(reader)  # Read the second line as the value
        for header, value in zip(headers, values):
            data_dict[header] = value


def files_process(file_path, posi_list, dic_a):
    data = pd.read_csv(file_path)
    columns = data.columns[1:]
    # print(data.columns[0:])
    for posi in columns:
        a, b = posi.split("_")
        if (float(a), float(b)) in posi_list:
            dic_a[(float(a), float(b))] = data[posi].tolist()
            # print(sum(data[posi].tolist()))


def postion_record(file, pos_list):
    data = pd.read_csv(file)
    lon_column = data['lon_all']
    lat_column = data['lat_all']
    length = len(lon_column)
    for i in range(length):
        lon = lon_column.iloc[i]
        lat = lat_column.iloc[i]
        pos_list.append((lon, lat))
        # print(lon,lat)


#  杰卡德处理
def Jaccard_processYear(posi1, posi2, data_dict, OneYearDict, numer_dic, deno_dic):
    # if posi1 in data_dict:
    #     data1 = data_dict[posi1]
    # else:
    #     print(f"Key {posi1} not found in data_dict")

    position1 = str(posi1[0]) + "_" + str(posi1[1])
    position2 = str(posi2[0]) + "_" + str(posi2[1])

    arr1 = np.array(data_dict[posi1])
    arr2 = np.array(data_dict[posi2])
    arr = np.vstack((arr1, arr2))  # Stack two arrays into a two-dimensional array

    result_d = np.sum(arr1 & arr2)  # Calculate the sum of the bitwise and result
    # calculate result_c
    result_c = np.sum(np.any(arr == 1, axis=0))

    all_zeros_arr1 = np.all(arr1 == 0)
    all_zeros_arr2 = np.all(arr2 == 0)

    default_value = -1

    if not all_zeros_arr1 and not all_zeros_arr2:
        default_value = result_d / result_c

    elif all_zeros_arr1 and all_zeros_arr2:
        default_value = -2

    OneYearDict[(position1, position2)] = default_value
    deno_dic[(position1, position2)] = result_c
    numer_dic[(position1, position2)] = result_d

    if posi1 != posi2:
        new_position = (position2, position1)

        default_value = -1

        if not all_zeros_arr1 and not all_zeros_arr2:
            default_value = result_d / result_c

        elif all_zeros_arr1 and all_zeros_arr2:
            default_value = -2

        OneYearDict[new_position] = default_value
        deno_dic[new_position] = result_c
        numer_dic[new_position] = result_d

    return result_d, result_c


def matrix_save_csv(data_dict, file_path):
    # create DataFrame
    SS = time.time()
    df = pd.DataFrame(list(data_dict.values()), index=pd.MultiIndex.from_tuples(data_dict.keys(), names=['x', 'y']))
    matrix_data = df.unstack(fill_value=0)
    # print(matrix_data)
    matrix_data.to_csv(file_path)
    EE = time.time()
    print("save success", EE - SS)


############################################
# position_list = position_list[:num_tag]

# distance： data_dict
# distance_dict = {}
# distance_find(distance_dict)
#####################################################


###################################################

def mainmian(input_folder, tag, color):
    # position：position_list
    position_list = []
    postion_record(posi_file, position_list)
    for file_name in os.listdir(input_folder):
        if file_name.endswith('.csv'):
            file_path = os.path.join(input_folder, file_name)
            a = file_name.replace('.csv', '')
        # if a != "2015_red" and a != "2100_red":
        #     continue
        # Put the extreme data for each year
        FinalData = {}
        files_process(file_path, position_list, FinalData)
        # print(FinalData)
        # deuce_list = []
        # The final Jaccard data for each year
        OneYearDict = {}
        numer_dict = {}  # molecule
        deno_dict = {}  # denominator
        i = 0
        tem_pos_save = []
        len_pos = len(position_list)
        print(len_pos)
        for mm in range(len_pos):
            pos1 = position_list[mm]
            # print(pos1)
            for j in range(mm, len_pos):
                pos2 = position_list[j]
                i += 1
                if i % 10000 == 1:
                    StartTime = time.time()

                # if (pos1,pos2) not in deuce_list:
                # print("jaccard")
                resultA, resultB = Jaccard_processYear(pos1, pos2, FinalData, OneYearDict, numer_dict, deno_dict)
                # print(i)
                if i % 10000 == 0:
                    EndTime = time.time()
                    print(i, a, resultA, resultB, pos1, pos2, "win", EndTime - StartTime)

        #########################################################

        # Define directory paths
        numer_dir = f"F:\\happy research\\data\\Jaccard\\{tag}\\numer\\{color}"
        deno_dir = f"F:\\happy research\\data\\Jaccard\\{tag}\\deno\\{color}"
        data_dir = f"F:\\happy research\\data\\Jaccard\\{tag}\\data\\{color}"

        # Creating a directory
        os.makedirs(numer_dir, exist_ok=True)
        os.makedirs(deno_dir, exist_ok=True)
        os.makedirs(data_dir, exist_ok=True)
        file1 = f"F:\\happy research\\data\\Jaccard\\{tag}\\numer\\{color}\\{a}_numer.csv"
        file2 = f"F:\\happy research\\data\\Jaccard\\{tag}\\deno\\{color}\\{a}_deno.csv"
        file3 = f"F:\\happy research\\data\\Jaccard\\{tag}\\data\\{color}\\{a}_data.csv"
        ############################################################
        matrix_save_csv(numer_dict, file1)
        matrix_save_csv(deno_dict, file2)
        matrix_save_csv(OneYearDict, file3)


change = ['ssp245']

color = ["orange", "red", "blue"]

posi_file = r"E:\happy research\geo\geo_result\geo3.csv"
# num_tag = 200
for i in change:
    for j in color:
        input_folder = f"F:\\happy research\\data\\{i}\\{i}_{j}"
        mainmian(input_folder, i, j)
