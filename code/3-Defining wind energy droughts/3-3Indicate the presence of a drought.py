from multiprocessing import Pool
import pandas as pd
import os
import time
from functools import partial


def mark_future(file_fu, df_fu, history_dic, diff_strands, outfolder):
    df_fu['time'] = pd.to_datetime(df_fu['time'])
    for ss in range(3):
        size = [12, 24, 36][ss]
        df_fu[f"mean_value_{size}"] = df_fu["cf_100"].rolling(window=size).mean().shift(-size).fillna(999)

        flag = []
        i = 0
        while i < len(df_fu):
            date = df_fu["time"].iloc[i]
            mean_value = df_fu[f"mean_value_{size}"].iloc[i]
            if mean_value == 999:
                break
            mon_hour1 = (date.month, date.hour)
            diff = mean_value - history_dic[mon_hour1][ss]
            if diff <= diff_strands[ss]:
                flag.append(1)
                i += 1
            else:
                flag.append(0)
                i += 1
        flag = flag + [0] * (len(df_fu) - len(flag))
        result = [0] * len(flag)
        num = 0
        while num < len(flag):
            if flag[num] == 1:
                # The current and following positions
                for j in range(num, min(num + size, len(flag))):
                    result[j] = 1
            num += 1
        df_fu[f"flag_{size}"] = result
    df_fu.to_csv(os.path.join(outfolder, file_fu), index=False)
    print(f"{file_fu} have succeeded")


def process_files(file_his, folder_his, folder_fu, outfolder):
    cf_his_dic = {}
    filepath_his = os.path.join(folder_his, file_his)
    sss = file_his.split("_")
    lon_lat = str(round(float(sss[2]), 3)) + "_" + str(round(float(sss[3].strip(".csv")), 3))
    df_his = pd.read_csv(filepath_his)
    df_his['time'] = pd.to_datetime(df_his['time'])

    for date, value12, value24, value36 in zip(df_his["time"], df_his["standard_12"], df_his["standard_24"],
                                               df_his["standard_36"]):
        mon_hour = (date.month, date.hour)
        if mon_hour not in cf_his_dic.keys():
            cf_his_dic[mon_hour] = (value12, value24, value36)
        if len(cf_his_dic.keys()) == 288:
            break

    filepaths_fu = os.listdir(folder_fu)
    for file_fu in filepaths_fu:
        sss1 = file_fu.split("_")
        lon_lat1 = str(round(float(sss1[1]), 3)) + "_" + str(round(float(sss1[2].strip(".csv")), 3))
        if lon_lat == lon_lat1:
            df_fu = pd.read_csv(os.path.join(folder_fu, file_fu))
            print(f"....{file_fu} have read")
            flags = [-0.2173966870785228, -0.2422955057324143, -0.326771918921619]
            mark_future(file_fu, df_fu, cf_his_dic, flags, outfolder)
            break


if __name__ == "__main__":
    start_time = time.time()
    folder_his = r"E:\happy research\data\extreme_a"
    folder_fu = r"E:\happy research\data\SSP585\ssp585_cf"
    outfolder = r"E:\happy research\data\SSP585\ssp585_flag"
    os.makedirs(outfolder, exist_ok=True)

    filepaths_his = os.listdir(folder_his)
    pool = Pool()

    for file_his in filepaths_his:
        print(f"{file_his} doing....")
        pool.apply_async(process_files, args=(file_his, folder_his, folder_fu, outfolder))
    pool.close()
    pool.join()
    print("Processing complete.")
    print(f"time need: {time.time() - start_time:.2f} s")


