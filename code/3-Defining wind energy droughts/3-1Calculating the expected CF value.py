import os
import time
import pandas as pd
from multiprocessing import Pool



def ave_extreme(filepath):
    df = pd.read_csv(filepath)
    df["time"] = pd.to_datetime(df["time"])
    ss = [12, 24, 36]
    for s in ss:
        df[f"mean_{s}"] = df["cf_100"].rolling(window=s).mean().shift(-s+1).fillna(999)
        time_cf_dic = {}
        for date, cf in zip(df["time"], df[f"mean_{s}"]):
            mon = date.month
            hour = date.hour
            if cf == 999:
                break
            if (mon, hour) not in time_cf_dic.keys():
                time_cf_dic[(mon, hour)] = [cf]
            else:
                time_cf_dic[(mon, hour)].append(cf)
        average_dic = {key: sum(values) / len(values) for key, values in time_cf_dic.items()}
        stand = []
        diff = []
        for date, cf in zip(df["time"], df[f"mean_{s}"]):
            mon = date.month
            hour = date.hour
            if cf == 999:
                stand += [999]*(s-1)
                diff += [0]*(s-1)
                break
            stand.append(average_dic[(mon, hour)])
            diff.append(cf - average_dic[(mon, hour)])
        df[f"standard_{s}"] = stand
        df[f"diff_{s}"] = diff
    df.to_csv(os.path.join(new_folder, file), index=False)
    print(f"{os.path.join(new_folder,file)} have succeeded")


# Add file path
if __name__ == "__main__":
    folder = r"E:\happy research\data\HIS\his_cf"
    new_folder = r"E:\happy research\data\HIS\his_diff_cf"
    os.makedirs(new_folder, exist_ok=True)
    # pool = Pool()
    filepaths = os.listdir(folder)
    count = 0
    for file in filepaths:
        count += 1
        start_time = time.time()
        filepath = os.path.join(folder, file)
        ave_extreme(filepath)
        # pool.apply_async(ave_extreme, args=(filepath,))
        print(f"{count}-----------{file} has reading...need time: {time.time() - start_time:.2f} s")
        # print(f"need time: {time.time() - start_time:.2f} s")
    # pool.close()
    # pool.join()

