import pandas as pd


def csv_to_artist_freq(path):
    df = pd.read_csv(path)
    id_artist_table = df[['p_id', 'artist_id']]
    artist_freq = df['artist_id'].value_counts()
    return id_artist_table, artist_freq

def playlist_top_k_artist_freq(id_artist_table, artist_freq,k=2000):
    top_k_artist = artist_freq[:k]

    top_K_artist_history_df =  id_artist_table.loc[id_artist_table['artist_id'].isin(top_k_artist.index)]
    top_K_artist_history_df = top_K_artist_history_df.reset_index()
    del top_K_artist_history_df['index']

    test = top_K_artist_history_df.groupby('p_id')['artist_id'].value_counts().astype('int')
    test = test.rename('playlist_artist_freq')
    test = test.reset_index(level='artist_id')
    return test

def (dt,N):

    test_df = pd.pivot_table(test.loc[:10000000], index='p_id', columns='artist_id', values='playlist_artist_freq')
    test_df.corr()
if __name__ == '__main__':
    exit()