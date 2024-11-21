import os
import pandas as pd


def calculate_wind_capacity_factor(speed, height, alpha):
    speed = speed * ((height / 10) ** alpha)
    if speed <= 3:
        return 0
    elif speed >= 14.5 and speed <= 25:
        return 1
    elif speed > 25:
        return 0
    elif speed >= 3 and speed <= 6:
        k = 1.78 * (speed ** 5) - 40.636 * (speed ** 4) + 365.88 * (speed ** 3) - 1604.4 * (
                speed ** 2) + 3460 * speed - 2960.184
        k = k / 1500  # Capacity factor calculation
        return k
    elif speed > 6 and speed <= 9:
        k = (-2.93333) * (speed ** 5) + 107.7 * (speed ** 4) - 1573.27 * (speed ** 3) + 11460.158 * (
                speed ** 2) - 41528.1 * speed + 59908.2
        k = k / 1500  # Capacity factor calculation
        return k
    else:
        k = 0.15247 * (speed ** 6) - 11.090005 * (speed ** 5) + 334.2735 * (speed ** 4) - 5339.027 * (
                speed ** 3) + 47583.517 * (speed ** 2) - 223812 * speed + 433774.2
        k = k / 1500  # Capacity factor calculation
        return k


def get_alpha():
    df_alpha = pd.read_csv('E:\happy research\geo\geo_result\geo3.csv')
    dic_alpha = {}
    for index, row in df_alpha.iterrows():
        lon_lat = str(row["lon_all"])+"_"+str(row["lat_all"])
        alpha = row["alpha"]
        dic_alpha[lon_lat] = alpha
    print(dic_alpha)
    folder = r"E:\happy research\data\HIS\csv_speed\his_hebing_linear"
    outfolder = r"E:\happy research\data\HIS\his_cf"
    os.makedirs(outfolder, exist_ok=True)
    filepaths = os.listdir(folder)
    count = 0
    for file in filepaths:
        count += 1
        df = pd.read_csv(os.path.join(folder, file))
        lon = file.split(".csv")[0].split("_")[1]
        lat = file.split(".csv")[0].split("_")[2]
        lon_lat_name = str(lon) + "_" + str(lat)
        alpha_num = dic_alpha[lon_lat_name]
        df["speed100m"] = df[df.columns[1]] * ((100 / 10) ** alpha_num)
        df['cf_100'] = df[df.columns[1]].apply(lambda x: calculate_wind_capacity_factor(x, 100, alpha_num))
        df.to_csv(os.path.join(outfolder, file), index=False)
        print(f"{count}---------------{file} have succeeded")


get_alpha()
