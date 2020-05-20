import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from pathlib import Path
from numba import jit
from numba import njit

pd.options.mode.chained_assignment = None  # default='warn'

from IPython import get_ipython
get_ipython().magic('time')
get_ipython().magic('timeit')

#%%
# import data
folder = Path('/Users/alex/Google Drive/04_Cloud/01_Work/Academia/01_Publications/00_Alex/005_RNC2')
edge_file = folder / 'data' / 'Copy of edge_data.parquet'
wind_file = folder / 'data' / 'Copy of GD_wind.csv'

wind_data = pd.read_csv(wind_file) 
edges = pd.read_parquet(edge_file, engine='pyarrow')

#%%

def wind_scenario(wind_data):
    i = np.random.randint(0, wind_data.values.shape[0])
    w = wind_data.values[i, 2]
    dist = wind_data.values[i, 1]
    b = wind_data.values[i, 3]
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


def ignition(d_probability, d_rng, d_rawSources, d_rawTargets):
    l_activated_sources = []
    l_activated_targets = []
    for idx_p, p in enumerate(d_probabilitiy):
        if p > d_rng[idx_p]:
            l_activated_sources.append(d_rawSources[idx_p])
            l_activated_targets.append(d_rawTargets[idx_p])
    return l_activated_sources, l_activated_targets


def propagation(previouslyActivatedTargets, d_sources, d_targets):
    l_newSources = []
    l_newTargets = []
    for idx_s, s in enumerate(d_sources):
        for idx_trgt_prev, trgt_prev in enumerate(previouslyActivatedTargets):
            if s == trgt_prev:
                l_newSources.append(s)
                l_newTargets.append(d_targets[idx_s])
    return l_newSources, l_newTargets
                

def mask(rawSources, rawTargets, d_bearing, d_distance, d_w_bearing_max, d_w_bearing_min, d_w_distance, d_sources, d_targets, d_allPreviousActivations):
    l_activated_sources = []
    l_activated_targets = []
    for idx_s, s in enumerate(rawSources):
        t = rawTargets[idx_s]
        for d_s, d_t in zip(d_sources, d_targets):
            if (s == d_s) & (t == d_t):
                if (bearing[idx_s] < d_w_bearing_max) & (bearing[idx_s] > d_w_bearing_min):
                    if distance[idx_s] < d_w_distance:
                        if s in d_allPreviousActivations:
                            l_activated_sources.append(s)
                            l_activated_targets.append(t)
    return l_activated_sources, l_activated_targets

#%%
for scenario in range(n):
    allPreviousActivations = []
    list_of_Activations = []
    # edges attributes
    sources = edges.source.values
    targets = edges.target.values
    distance = edges.distance.values
    bearing = edges.bearing.values
    probability = edges.bearing.values
    # initial setup
    condition = True
    time = 0
    rng = np.random.uniform(0, 1, size=probability.shape[0])
    w_bearing_max, w_bearing_min, w_distance = wind_scenario(wind_data)
    # ignition
    activated_sources, activated_targets = ignition(probability, rng, sources, targets)
    if activated_sources.empty:
        condition = False
    while condition:
