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

edge_data = os.path.join(folder,'data','Copy of edge_data.parquet')
wind_data = os.path.join(folder,'data','Copy of GD_wind.csv ')

edges = pd.read_parquet(edge_data, engine='pyarrow')

#%%
@jit
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


def display_network(edge_list_dataframe): 
    graph = nx.from_pandas_edgelist(edge_list_dataframe, edge_attr=True) # display edge list as network
    options = {'node_color': 'red', 'node_size': 50, 'width': 1, 'alpha': 0.4,
               'with_labels': False, 'font_weight': 'bold'}
    nx.draw_kamada_kawai(graph, **options)
    plt.show()
    return graph



def create_contact_array(edgelist):
    return np.full((edgelist.values.shape[0],1), True) # create contact array on the same index as edge list



def initial_conditions(edgelist):
    rng = np.random.uniform(0, 1, size=edgelist.values.shape[0])
    return rng < edgelist.IgnProb_bl.values  # conditions, return a boolean



def propagation_conditions(edgelist, contact_array, bear_max, 
                           bear_min, dist):
    boolean1 = (edgelist.distance.values < dist)  # compare with distance between building
    boolean2 = (edgelist.bearing.values < bear_max) and (edgelist.bearing.values > bear_min)  # compare with bearig between buildings
    boolean3 = np.any(contact_array == True, axis=1) # columns where any elements are True ~ is it already burnt ?
    return boolean1 & boolean2 & boolean3# create boolean mask for all conditions
    return boolean1 & boolean2 & boolean3
    


def update_contacts(contact_array, boolean_array): # update a new contact time column
    return np.c_[contact_array, boolean_array]

    
def filter_edgelist_at_time(edgelist, contact_array, time): # new edges list at time
    return edgelist.values[contact_array[:, time] == True]


#%%
n_scenario = 2
list_of_fires = []
for scenario in range(n_scenario):
    # initial conditions
    time = 0
    wind_bearing_max, wind_bearing_min, wind_distance = wind_scenario(wind_data) # wind conditions
    ignition_bool = initial_conditions(edges)  # ignition conditions
    # keep track of active edges
    contacts = create_contact_array(edges)  # create contact array
    contacts[:, 0] == ignition_bool  # just for time = 0
    # active edgelist
    fires = filter_edgelist_at_time(edges, contacts, time)
    list_of_fires.append(fires)
    while (True in contacts[:, -1]):  # start time stepping
        time += 1
        propagation_bool = propagation_conditions(fires, contacts, 
                                                  wind_bearing_max, 
                                                  wind_bearing_min, 
                                                  wind_distance)
        contacts = update_contacts(contacts, propagation_bool)
        fires = filter_edgelist_at_time(edges,contacts, time)
        list_of_fires.append(fires)
    else:
        da_scenario = da.concatenate(list_of_fires, axis=1)
    dd_scenario = dd.from_dask_array(da_scenario, columns=['source', 'target', 'distance', 'bearing', 'IgnProb_bl'])
    dd_scenario.to_parquet(folder / 'output' / 'scenario_'+scenario+'.parquet', engine='pyarrow')
            
    