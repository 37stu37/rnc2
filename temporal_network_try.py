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
import dask.dataframe as dd
import dask.array as da
import sys
import os
import networkx as nx
from numba import jit

from dask.distributed import Client
client = Client(n_workers=1, threads_per_worker=4, processes=False, memory_limit='6GB')


#%%
folder = "/Users/alex/Google Drive/04_Cloud/01_Work/Academia/01_Publications/00_Alex/005_RNC2"

edge_file = os.path.join(folder,'data','Copy of edge_data.parquet')
wind_file = os.path.join(folder,'data','Copy of GD_wind.csv')

edges = pd.read_parquet(edge_file, engine='pyarrow')

#%%
def wind_scenario(file_name): 
    wind_data = pd.read_csv(file_name) 
    i = np.random.randint(0, wind_data.shape[0])
    w = wind_data.iloc[i, 2]
    dist = wind_data.iloc[i, 1]
    b = wind_data.iloc[i, 3]
    bear_max = b + 45  # wind direction
    bear_min = b - 45
    if b == 360:
        bear_max = 45
    if b <= 0:  # should not be necessary
        bear_min = 0
    if b == 999:
        bear_max = 999
        bear_min = 0
    return bear_max, bear_min, dist # wind characteristics, bearing and distance


def conditions_at_time(time_integer, edgelist, contact_matrix, 
                      wind_direction_max, wind_direction_min,
                      wind_distance):
    if time_integer == 0:  # set initial conditions
        rng = np.random.uniform(0, 1, size=edgelist.values.shape[0])
        mask = rng < edges.IgnProb_bl.values
        return mask    
    else:
        # source -> target
        boolean0 = (edgelist[:,0]) == (edgelist[contact_matrix[:,-1] == True][:,1])
        # wind direction
        boolean1 = (edgelist[:,3] < wind_direction_max) & (edgelist[:,3] > wind_direction_min)
        # wind distance
        boolean2 = edgelist[:,2] < wind_distance
        # already burnt
        boolean3 = np.any(contact_matrix[:, :-1] == True, axis=1)
        # create mask
        mask = boolean1 & boolean2 & boolean3
        return mask


def valid_edges_at_time(time, edgelist, contact_matrix, boolean_array):
    c = np.c_[contact_matrix, boolean_array]
    e = edgelist[contact_matrix[:, time]]
    return c, e


def main(edgelist, n_scenarios):
    list_of_activation = []
    contact_matrix = np.full((edgelist.shape[0],1), True)
    wind_bearing_max, wind_bearing_min, wind_distance = wind_scenario(wind_file)
    for scenario in range(n_scenarios):
        time = 0
        print('this is scenario : /n', scenario, 'time step : /n', time)
        while (True in contact_matrix[:, -1]):
            boolean_mask = conditions_at_time(time, edgelist, contact_matrix, 
                                         wind_bearing_max, wind_bearing_min, 
                                         wind_distance)
            contact_matrix, active_edges = valid_edges_at_time(time, edgelist,
                                                               contact_matrix, boolean_mask)
            list_of_activation.append(active_edges)
            time += 1
        else:
            da_scenario = da.concatenate(list_of_fires, axis=1)
        dd_scenario = dd.from_dask_array(da_scenario, columns=['source', 'target', 'distance', 'bearing', 'IgnProb_bl'])
        dd_scenario.to_parquet(os.path.join(folder,'output','scenario_{}.parquet'.format(scenario), engine='pyarrow')              