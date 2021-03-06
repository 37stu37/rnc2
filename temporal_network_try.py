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

@jit
def ignition(d_probability, d_rng, d_rawSources, d_rawTargets):
    l_activated_sources = []
    l_activated_targets = []
    for idx_p, p in enumerate(d_probability):
        if p < d_rng[idx_p]:
            l_activated_sources.append(d_rawSources[idx_p])
            l_activated_targets.append(d_rawTargets[idx_p])
    return l_activated_sources, l_activated_targets

@jit
def propagation(act_targets, rawSources, rawTargets):
    l_newSources = []
    l_newTargets = []
    for idx_s, s in enumerate(rawSources):
        for idx_trgt_prev, trgt_prev in enumerate(act_targets):
            if s == trgt_prev:
                l_newSources.append(s)
                l_newTargets.append(rawTargets[idx_s])
    return l_newSources, l_newTargets
                
@jit
def valid_edges(rawSources, rawTargets, d_bearing, d_distance, d_w_bearing_max, d_w_bearing_min,
                d_w_distance, act_sources, act_targets, 
                d_allPreviousActivations):
    if d_allPreviousActivations:
        return act_sources, act_targets
    else:
        l_activated_sources = []
        l_activated_targets = []
        for idx_s, s in enumerate(rawSources):
            t = rawTargets[idx_s]
            for idx_d_s, d_s in enumerate(act_sources):
                d_t = act_targets[idx_d_s]
                if (s == d_s) & (t == d_t):
                    if (bearing[idx_s] < d_w_bearing_max) & (bearing[idx_s] > d_w_bearing_min):
                        if distance[idx_s] < d_w_distance:
                            if s not in d_allPreviousActivations:
                                l_activated_sources.append(s)
                                l_activated_targets.append(t)
        return l_activated_sources, l_activated_targets


def lists_to_arrays(list1, list2, scenario, time):
    list_scenario = [scenario] * len(list1)
    list_time = [time] * len(list1)
    return np.transpose([list1, list2, list_scenario, list_time])
#%%
n = 1
# attributes
sources = edges.source.values
targets = edges.target.values
distance = edges.distance.values
bearing = edges.bearing.values
probability = edges.bearing.values
for scenario in range(n):
    Recordings = []
    allActivated_sources = []
    # initial setup
    condition = True
    time = 0
    rng = np.random.uniform(0, 1, size=probability.shape[0])
    w_bearing_max, w_bearing_min, w_distance = wind_scenario(wind_data)
    # ignition
    activated_sources, activated_targets = ignition(probability, rng, sources, targets)
    print("activated_sources: {}, activated_targets : {}". format(len(activated_sources), len(activated_targets)))
    if not activated_sources:
        print("activated_sources empty")
        continue
    while condition:
        # mask
        activated_sources, activated_targets = valid_edges(sources, targets, bearing, distance, w_bearing_max, w_bearing_min, w_distance, activated_sources, activated_targets, allActivated_sources)
        print("activated_sources: {}, activated_targets : {}". format(len(activated_sources), len(activated_targets)))
        # store results
        activated_array = lists_to_arrays(activated_sources,activated_targets, scenario, time)
        Recordings.append(activated_array)
        allActivated_sources.extend(activated_sources)
        # propagation time + 1
        activated_sources, activated_targets = propagation(activated_targets, sources, targets)
        print("activated_sources: {}, activated_targets : {}". format(len(activated_sources), len(activated_targets)))