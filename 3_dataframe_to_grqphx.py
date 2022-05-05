import os
import sys
import itertools

from tqdm import tqdm

import pandas as pd
import numpy as np
import json

import networkx as nx

def df_to_arcs(path):
    df = pd.read_csv(path, index_col=0)
    groupby = df.groupby('p_id')['artist_id'].value_counts().astype('float')
    groupby = groupby.reset_index(level='artist_id')
    node_list = []

    for x in tqdm(groupby.index.unique()):
        temp = groupby.loc[x]
        arcs = list(itertools.combinations(temp['artist_id'], 2))
        arcs = list(itertools.chain(*arcs))
        node_list.append(arcs)

    arcs_array = np.array(node_list, dtype='str')
    arcs_array = arcs_array.reshape((arcs_array.shape[0] // 2, 2))

    return arcs_array

def node_weight(arcs_array):
    nodes = list(np.unique(arcs_array))
    nodes = list(map(lambda x: [x, {'weight': 0}], nodes))
    return nodes

def to_networkx(arcs_array):
    G = nx.Graph()
    nodes = node_weight(arcs_array)
    G.add_nodes_from(nodes)

def save_graph_json():
    nx.write_gml(G, "path_where_graph_should_be_saved.gml")

if __name__ == '__main__':

    exit()