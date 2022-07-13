import os
import sys
import time
import multiprocessing
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
    df_list = []
    for x in dir_list:
        df_list.append(pd.read_csv(x, index_col=0))

    df = pd.concat(df_list,axis=1)
    df = df.drop_duplicates()
    return df


if __name__ == '__main__':
    start_time = time.time()

    datafile_dir = 'spotify_splited'
    file_dir_list = os.listdir(datafile_dir)

    for x in file_dir_list:
        print(x)
    sys.exit()
    # file_dir_list = list(map(lambda x: '{}/{}'.format(datafile_dir, x), file_dir_list))

    num_cpus = get_num_cpus()

    input_data = multi_processing_setting(datafile_dir, num_cpus)


    sys.exit()
    p = Pool(num_cpus)
    output = p.map(arrange_artist, input_data)
    df = pd.concat(output, axis=1)
    df = df.drop_duplicates()
    df.to_csv('Artist_id.csv')
    p.close()
    p.join()

    end_time = time.time()
    print('Spending Time is {} Minute '.format(round((end_time - start_time) / 60, 2)))

    sys.exit()