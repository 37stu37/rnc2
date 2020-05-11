# -*- coding: utf-8 -*-
"""temporal_network_try.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1kptG5xUQkG2GChLgAI7TaMBj2ulwl_lu
"""

# Commented out IPython magic to ensure Python compatibility.
# %matplotlib inline
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys
import os
import networkx as nx
from numba import jit

"""**Tentative code for rnc2 temporal network**

* the idea is to set a maximaly connected network from the start
* the network is an adjacency list where we add a third column as a "contact time" list
* contact times represent the set of active connections at a certain time
* static network evolve through time by filtering by the acitve connection at every time step

** Test with FFE edge list **


---
"""

#%%
path = '/content/drive/My Drive/05_Sync/FFE/FireNetwork/00_input'
path_output = '/content/drive/My Drive/05_Sync/FFE/FireNetwork/00_output'
# with open(r'/content/drive/My Drive/05_Sync/FFE/FireNetwork/00_output/dask_edge_list/edge_data.parquet', 'rb') as f:
#     edges = pd.read_parquet(f, engine='pyarrow')
EDGES = pd.read_parquet(os.path.join(path_output, 'dask_edge_list', 'edge_data.parquet'), engine='pyarrow')

#%%
# wind scenario
def wind_scenario(file_name):
    # wind scenario conditions
    wind_data = pd.read_csv(os.path.join(path, file_name))
    i = np.random.randint(0, wind_data.shape[0])
    w = wind_data.iloc[i, 2]
    d = wind_data.iloc[i, 1]
    b = wind_data.iloc[i, 3]
    # wind direction
    wind_bearing_max = b + 45
    wind_bearing_min = b - 45
    if b == 360:
        wind_bearing_max = 45
    if b <= 0:  # should not be necessary
        wind_bearing_min = 0
    if b == 999:
        wind_bearing_max = 999
        wind_bearing_min = 0
    
    return wind_bearing_max, wind_bearing_min, d