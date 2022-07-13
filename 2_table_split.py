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


def dataset_split(dir_list, drop_duplicates = True):
    table_columns = [
        ['id', 'p_id', 'num_follower'],
        ['p_id', 'artist'],
        ['track', 'track_id', 'duration'],
        ['artist_id', 'artist'],
        ['album_id', 'album']
    ]

    for before_dir in dir_list:
        entire_df = pd.read_csv(before_dir, index_col=0)
        splited_csvs = ['user_id_name', 'userid_artist', 'track_id_name', 'artist_id_name', 'album_id_name']
        if drop_duplicates:
            entire_df.drop_duplicates()
        for after_dir, columns in zip(splited_csvs, table_columns):
            entire_df[columns].to_csv('spotify_splited/{}/{}'.format(after_dir, before_dir.split('/')[1]))

    return


if __name__ == '__main__':
    start_time = time.time()

    create_directory('spotify_splited')

    splited_csvs =['user_id_name', 'userid_artist', 'track_id_name', 'artist_id_name', 'album_id_name']

    for x in splited_csvs:
        create_directory('spotify_splited/{}'.format(x))

    datafile_dir = 'spotify_csv'
    file_dir_list = os.listdir(datafile_dir)
    file_dir_list = list(map(lambda x: '{}/{}'.format(datafile_dir, x), file_dir_list))

    maximum = int(multiprocessing.cpu_count())

    num_cpus = int(input("Set the number of Cpus (Maximum number of this devise : {} ) ".format(maximum)))
    while num_cpus > maximum:
        num_cpus = int(input("Set the number of Cpus (Maximum number of this devise : {} ) ".format(maximum)))

    if num_cpus == 1:
        unit_size = len(file_dir_list)
    else:
        unit_size = int(len(file_dir_list) // (num_cpus))

    multiprocess_list = []

    for x in range(num_cpus):
        start = x*unit_size
        end = (x+1)*unit_size
        if x == num_cpus-1:
            multiprocess_list.append(file_dir_list[start:])
        else:
            multiprocess_list.append(file_dir_list[start:end])

    p = Pool(num_cpus)
    output = p.map(dataset_split,multiprocess_list)
    p.close()
    p.join()

    sys.exit()
