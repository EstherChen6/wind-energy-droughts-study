
import pandas as pd
import time
import operator
#'2015-01-01 00:30:00'

import os

start_time=time.time()
import asyncio
import os
import pandas as pd

from datetime import datetime, timedelta
import pandas as pd

from datetime import datetime, timedelta


def hours_since_1950(date_str):
    """
    Calculate the total number of hours between the given date and time (e.g. "2014-12-30 01:30:00") and January 1st, 1950 at 0:30.
    Add 1 for each hour interval passed.

    Parameters:
    date_str (str): ISO 8601 date and time in string format.

    Return:
    int: Total hours relative to January 1, 1950 at 0:30.
    """

    # Define the starting point
    start_date = datetime(1950, 1, 1, 0, 30)

    # Parse time strings in ISO 8601 format directly
    input_date = datetime.fromisoformat(date_str)

    # Calculate the time difference and convert it to total hours
    delta_hours = (input_date - start_date) / timedelta(hours=1)

    # Because the starting point is 0:30, the first hour needs to be subtracted by 0.5 hours (i.e. 30 minutes) to ensure that the timing starts from 0.
    return int(delta_hours) if delta_hours > 0 else 0

async def build_dict_from_csv_async(csv_file):
    print(csv_file)
    a, b, c, d, e = csv_file.split('_')
    e = e[:-4]
    df = pd.read_csv(csv_file)
    df['d'] = d
    df['e'] = e
    df['time_true'] = df['time'].apply(hours_since_1950)
    result_dict = dict(zip(zip(df['d'], df['e'], df['time_true']), df['cf_100_36_diff']))
    result_dict1 = {k: v for k, v in result_dict.items() if v < -0.002}
    return result_dict1


async def process_files(input_folder):
    dic_all = {}
    tasks = []

    for i in os.listdir(input_folder):
        if i.endswith('.csv'):
            filepath = os.path.join(input_folder, i)
            task = build_dict_from_csv_async(filepath)
            tasks.append(task)

    results = await asyncio.gather(*tasks)
    for result in results:
        dic_all.update(result)

    return dic_all


input_folder = r'E:\happy research\data\extreme_a'

# Dictionary: keys are address and time (lon,lat,time), values are ▲cf
# time starts at 1950, the first time is 0, and so on
# The first time point in 1951 is 8760
# The dictionary is named dict_cf
import bisect

def select(sorted_list, quan):
    index = int((len(sorted_list) - 1) * quan)
    # Determines whether the index is an integer
    if index % 1 == 0:
        quantile = sorted_list[index]
    else:
        lower_value = sorted_list[int(index)]
        upper_value = sorted_list[int(index) + 1]
        quantile = (lower_value + upper_value) / 2
    return quantile


import bisect


class TimeRangeManager:
    total_length = 0

    def __init__(self):
        self.time_ranges = []

    def add_number(self, num):
        start = num
        end = num + 35

        # Finding the insertion position
        pos = bisect.bisect_left(self.time_ranges, (start, end))

        merge_start, merge_end = start, end
        old_length = 0  # The total length before merging is recorded

        # Merge the left time periods
        left_pos = pos - 1
        while left_pos >= 0:
            s, e = self.time_ranges[left_pos]
            if e < merge_start:  # Completely on the left
                break
                # There's overlap, there's merging
            merge_start = min(merge_start, s)
            merge_end = max(merge_end, e)
            old_length += (e - s + 1)  # Record the old interval length
            left_pos -= 1

            # Merge the right time periods
        right_pos = pos
        while right_pos < len(self.time_ranges):
            s, e = self.time_ranges[right_pos]
            if s > merge_end:  # Completely on the right
                break
                # There's overlap, there's merging
            merge_start = min(merge_start, s)
            merge_end = max(merge_end, e)
            old_length += (e - s + 1)  # Record the old interval length
            right_pos += 1

            # Update time period
        new_ranges = self.time_ranges[:left_pos + 1] + [(merge_start, merge_end)] + self.time_ranges[right_pos:]

        self.time_ranges = new_ranges

        # Update total length
        new_length = (merge_end - merge_start + 1)
        TimeRangeManager.total_length += new_length - old_length

    def get_time_ranges(self):
        return self.time_ranges

    @classmethod
    def total_coverage(cls):
        return cls.total_length


# Run the main logic asynchronously
async def main():
    dic_all = await process_files(input_folder)
    start_time2=time.time()
    print(f'async need {start_time2-start_time} s')
    # dict_cf = {('a1', 'a2', 1): 2, ('a1', 'a2', 2): 1, ('c1', 'c2', 6): 2, ('d1', 'd2', 3): 4, ('e1', 'e2', 7): 7}
    dict_true = dict(sorted(dic_all.items(), key=operator.itemgetter(1)))
    # print(dict_true)
    value_list = list(dict_true.values())
    start_time3=time.time()
    print(f'sort need {start_time3-start_time2} s')
    dic_final = {}
    time1 = 569785 * 1735 * 0.01  # The total number of hours, we should be 1% of the total time
    flag = -1

    for key, value in dict_true.items():
        flag += 1
        print(flag)
        ll=TimeRangeManager.total_coverage()
        print(ll)
        kk = (key[0], key[1])
        if ll <= time1:
            if kk not in dic_final.keys():
                dic_final[kk] = TimeRangeManager()
                dic_final[kk].add_number(key[2])
            else:
                dic_final[kk].add_number(key[2])
        else:
            print(value_list[flag - 2])
            break
    # print(dic_final)
    print(f'async need {start_time2 - start_time} s')
    print(f'sort need{start_time3 - start_time2} s')
    print(f'last need {time.time() - start_time3} s')
    print(f'total time is {time.time() - start_time}')

    print(TimeRangeManager.total_coverage())


# asyncio.run()  run asynchronous functions directly in Python 3.7+
asyncio.run(main())


# print(value_list)





