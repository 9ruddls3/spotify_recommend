import os
import sys
import json
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


def json_parsing(file_dir):
    restructed_dict = {
        'id': [],
        'p_id': [],
        'num_follower': [],
        'track': [],
        'track_id': [],
        'duration': [],
        'artist': [],
        'artist_id': [],
        'album': [],
        'album_id': []
    }
    with open(file_dir,'r') as o:
        content = json.loads(o.read())
        for it in content['playlists']:
            for track_info in it['tracks']:
                restructed_dict['id'].append(it['name'])
                restructed_dict['p_id'].append(it['pid'])
                restructed_dict['num_follower'].append(it['num_followers'])
                restructed_dict['track'].append(track_info['track_name'])
                restructed_dict['track_id'].append(track_info['track_uri'].split(':')[2])
                restructed_dict['duration'].append(track_info['duration_ms'])
                restructed_dict['artist'].append(track_info['artist_name'])
                restructed_dict['artist_id'].append(track_info['artist_uri'].split(':')[2])
                restructed_dict['album'].append(track_info['album_name'])
                restructed_dict['album_id'].append(track_info['album_uri'].split(':')[2])
    return pd.DataFrame(restructed_dict)


def data_restructure(file_dir_list):
    for x in file_dir_list:
        csv_id = x.split('.')[2]
        json_parsing(x).to_json('spotify_json/{}.json'.format(csv_id), orient = 'split',)
    return


if __name__ == '__main__':
    start_time = time.time()
    datafile_dir = 'spotify_million_playlist_dataset/data'
    file_dir_list = os.listdir(datafile_dir)
    file_dir_list = list(map(lambda x: datafile_dir+'/'+x,file_dir_list))

    create_directory('spotify_json')

    file_dir_list = os.listdir(datafile_dir)
    file_dir_list = list(map(lambda x: '{}/{}'.format(datafile_dir, x), file_dir_list))

    num_cpus = get_num_cpus()

    input_data = multi_processing_setting(datafile_dir, num_cpus)

    p = Pool(num_cpus)
    output = p.map(data_restructure, input_data)
    p.close()
    p.join()

    end_time = time.time()
    print('Spending Time is {} Minute '.format(round((end_time - start_time) / 60, 2)))

    sys.exit()