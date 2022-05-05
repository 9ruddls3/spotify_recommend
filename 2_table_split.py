import os
import pandas as pd
from tqdm import tqdm

def createDirectory(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError\
            : print("Error: Failed to create the directory.")

if __name__ == '__main__':
    createDirectory('spotify_splited')
    dir = 'spotify_splited'

    datafile_dir = 'spotify_csv'
    file_dir_list = os.listdir(datafile_dir)
    file_dir_list = list(map(lambda x: datafile_dir + '/' + x, file_dir_list))

    entire_dataset = []

    entire_dataset = list(map(lambda x: pd.read_csv(x), file_dir_list))

    df = pd.concat(entire_dataset)
    del entire_dataset

    df.to_csv('entire_dataset.csv', encoding="utf-8")

    tables = {
        'user_id_name': df[['id', 'p_id', 'num_follower']],
        'track_id_name': df[['track', 'track_id', 'duration']],
        'artist_id_name': df[['artist_id', 'artist']],
        'album_id_name': df[['album_id', 'album']]}

    for x in tqdm(tables):
        tables[x] = tables[x].drop_duplicates()
        tables[x].to_csv('{}/{}.csv'.format(dir,x))
        del tables[x]

    df[['p_id', 'track_id', 'artist_id', 'album_id']].to_csv('{}/id_dataset.csv'.format(dir), encoding="utf-8")

    print('data restructure is complete!')
