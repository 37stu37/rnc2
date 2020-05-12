# -*- coding: utf-8 -*-
"""
**Tentative code for rnc2 temporal network**

* the idea is to set a maximaly connected network from the start
* the network is an adjacency list where we add a third column as a "contact time" list
* contact times represent the set of active connections at a certain time
* static network evolve through time by filtering by the acitve connection at every time step

** Test with FFE edge list **

"""

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import sys
import os
from pathlib import Path
import networkx as nx
from numba import jit

#%%
data_folder = Path("/Users/alex/Google Drive/04_Cloud/01_Work/Academia/01_Publications/00_Alex/005_RNC2/data")

edge = data_folder / 'Copy of edge_data.parquet'
wind = data_folder / 'Copy of GD_wind.csv '

edges = pd.read_parquet(edge, engine='pyarrow')

#%%


@jit
def wind_scenario(file_name): # wind characteristics, bearing and distance
    # wind scenario conditions
    wind_data = pd.read_csv(file_name)
    i = np.random.randint(0, wind_data.shape[0])
    w = wind_data.iloc[i, 2]
    dist = wind_data.iloc[i, 1]
    b = wind_data.iloc[i, 3]
    bear_max = b + 45 # wind direction
    bear_min = b - 45
    if b == 360:
        bear_max = 45
    if b <= 0:  # should not be necessary
        bear_min = 0
    if b == 999:
        bear_max = 999
        bear_min = 0
    return bear_max, bear_min, wind_distance
    
    return bear_max, bear_min, dist


def display_network(edge_list_dataframe): # display edge list as network
    graph = nx.from_pandas_edgelist(edge_list_dataframe, edge_attr=True)
    options = {'node_color': 'red', 'node_size': 50, 'width': 1, 'alpha': 0.4,
               'with_labels': False, 'font_weight': 'bold'}
    nx.draw_kamada_kawai(graph, **options)
    plt.show()
    return graph


@jit
def create_contact_array(e): # create contact array on the same index as edge list
    return np.full((e.values.shape[0],1), True)


@jit
def initial_conditions(e): # conditions, return a boolean
    rng = np.random.uniform(0, 1, size=e.values.shape[0])
    boolean = rng < e.IgnProb_bl.values


@jit
def update_contacts(contact_array, boolean_array): # update a new contact time column
    return np.c_[contact_array, boolean_array]
    

@jit
def filter_edgelist(e, contact_array): # new edges list at time
    return e.values[contacts[:, time] == True]

@jit
def main(e, n_scenario):
    for scenario in range(n_scenario):
        wind_b
    
    