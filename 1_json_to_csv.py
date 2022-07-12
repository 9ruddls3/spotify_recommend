import json
import sys
import time
import multiprocessing
from multiprocessing import Pool
import pandas as pd
import os


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
        json_parsing(x).to_csv('spotify_csv/{}.csv'.format(csv_id))
    return

def createDirectory(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError\
            : print("Error: Failed to create the directory.")

if __name__ == '__main__':
    start_time = time.time()
    datafile_dir = 'spotify_million_playlist_dataset/data'
    file_dir_list = os.listdir(datafile_dir)
    file_dir_list = list(map(lambda x: datafile_dir+'/'+x,file_dir_list))

    createDirectory('spotify_csv')

    maximum = int(multiprocessing.cpu_count())

    num_threads = int(input("Set the number of thrads (Maximum number of this devise : {} ) ".format(maximum)))

    while num_threads > maximum:
        num_threads = int(input("Set the number of thrads (Maximum number of this devise : {} ) ".format(maximum)))

    thread_list = []
    unit_size = int(len(file_dir_list)//num_threads)

    for x in range(num_threads-1):
        thread_list.append(file_dir_list[x*unit_size:(x+1)*unit_size])

    thread_list.append(file_dir_list[(num_threads-1)*unit_size:])
    p = Pool(num_threads)
    output = p.map(data_restructure,thread_list)
    p.close()
    p.join()

    sys.exit()