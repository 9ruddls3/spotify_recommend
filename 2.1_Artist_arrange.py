import os
import sys
import time
import multiprocessing
import parmap
import pandas as pd

from multiprocessing import Pool


def create_directory(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print("Error: Failed to create the directory.")


def get_num_cpus():
    maximum_cpus = int(multiprocessing.cpu_count())
    cpus = int(input("Set the number of Cpus (Maximum number of this devise : {} ) ".format(maximum_cpus)))

    while cpus > maximum_cpus:
        cpus = int(input("Set the number of Cpus (Maximum number of this devise : {} ) ".format(maximum_cpus)))

    return cpus


def multi_processing_setting(file_dir_list, num_cpu):
    if num_cpu == 1:
        unit_size = len(file_dir_list)
    else:
        unit_size = int(len(file_dir_list) // (num_cpu))

    multiprocess_list = []

    for x in range(num_cpu):
        start = x*unit_size
        end = (x+1)*unit_size
        if x == num_cpu-1:
            multiprocess_list.append(file_dir_list[start:])
        else:
            multiprocess_list.append(file_dir_list[start:end])

    return multiprocess_list


def arrange_artist(dir_list):
    return_freq_series = pd.Series()
    return_non_duplicate_series = pd.Series()
    for x in dir_list:
        temp_df = pd.read_csv(x, index_col=0)

        return_freq_series = return_freq_series.add(temp_df['artist'].value_counts())
        return_non_duplicate_series = return_non_duplicate_series.add(temp_df.drop_duplicates()['artist'].value_counts())

    return return_freq_series, return_non_duplicate_series


if __name__ == '__main__':
    start_time = time.time()

    datafile_dir = 'spotify_splited/artist_id_name'
    file_dir_list = os.listdir(datafile_dir)
    file_dir_list = list(map(lambda x: '{}/{}'.format(datafile_dir, x), file_dir_list))

    num_cpus = get_num_cpus()
    input_data = multi_processing_setting(file_dir_list, num_cpus)

    output = parmap.map(arrange_artist, input_data, pm_pbar=True, pm_processes=num_cpus)

    artist_freq_series = pd.Series()
    artist_non_duplicate_series = pd.Series()

    for x in output:
        artist_freq_series = artist_freq_series.add(x[0])
        artist_non_duplicate_series = artist_non_duplicate_series.add(x[1])

    df = pd.DataFrame(
        index=artist_freq_series.index,
        data={'freq': artist_freq_series, 'non_duplicate': artist_non_duplicate_series}
    )

    df.to_csv('Artist_freq.csv')

    end_time = time.time()
    print('Spending Time is {} Minute '.format(round((end_time - start_time) / 60, 2)))

    sys.exit()