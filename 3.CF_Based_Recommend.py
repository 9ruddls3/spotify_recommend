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


def CF_Recommendation(dir_list, drop_duplicates=True):

    for before_dir in dir_list:
        df = pd.read_csv(before_dir, index_col=0)
        splited_csvs = ['user_id_name', 'userid_artist', 'track_id_name', 'artist_id_name', 'album_id_name']
        if drop_duplicates:
            df.drop_duplicates()
        for after_dir, columns in zip(splited_csvs, table_columns):
            df[columns].to_csv('spotify_splited/{}/{}'.format(after_dir, before_dir.split('/')[1]))

    return


def multi_processing_setting(datafile_dir, num_cpu, maximum_cpus):
    file_dir_list = os.listdir(datafile_dir)
    file_dir_list = list(map(lambda x: '{}/{}'.format(datafile_dir, x), file_dir_list))

    maximum_cpus = int(multiprocessing.cpu_count())
    num_cpu = int(input("Set the number of Cpus (Maximum number of this devise : {} ) ".format(maximum)))

    while num_cpu > maximum_cpus:
        num_cpu = int(input("Set the number of Cpus (Maximum number of this devise : {} ) ".format(maximum)))

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

if __name__ == '__main__':
    start_time = time.time()

    maximum = int(multiprocessing.cpu_count())

    num_cpus = int(input("Set the number of Cpus (Maximum number of this devise : {} ) ".format(maximum)))
    while num_cpus > maximum:
        num_cpus = int(input("Set the number of Cpus (Maximum number of this devise : {} ) ".format(maximum)))

    input_data = multi_processing_setting(datafile_dir, num_cpus, maximum)

    p = Pool(num_cpus)
    output = p.map(CF_Recommendation, input_data)
    p.close()
    p.join()

    sys.exit()