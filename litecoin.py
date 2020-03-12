#!/usr/bin/env python
# coding: utf-8

# In[31]:


# necessary packages
import pandas as pd
from pandas.io.json import json_normalize
import gzip
import json
import multiprocessing as mp
import time
import copy
import pickle
import networkx
# from tqdm._tqdm_notebook import tqdm_notebook
# tqdm_notebook.pandas()


# ## Creating a DataFrame from the Dataset

# In[52]:


def convert_json(json_str):
#     converts json string to df
    return json_normalize(json.loads(json_str))

def convert_data(prefix, file_num):
    for i in range(file_num):
        suffix = str(i).zfill(12)
        filename = prefix + suffix
        
        with gzip.open(filename, "rt", encoding = "utf-8") as file:
            with mp.Pool() as pool:
                results = pool.map(convert_json, [line for line in file if line])
                mode = 'a' if i else 'w'
                pd.concat(results, sort=False).to_csv('data.csv', header = not i, mode = mode)        


# In[ ]:


# convert gunzipped json files from google bigquery to a single csv
# convert_data('data/2019_01_04_', 4)


# In[23]:


def load_data(filename):
    df = pd.read_csv(filename)
    return df
df = load_data('data.csv')


# ## Creating Address Clusters

# In[ ]:


# %time lists = [set(df[df['tx_hash'] == tx]['inputs_addresses']) for tx in set(df.head(10000)['tx_hash'])]

def get_input_addrs(tx):
    return set(df[df['tx_hash'] == tx]['inputs_addresses'])

def get_prelim_clusters(df):
	start = time.time()
	with mp.Pool() as pool:
	    prelim = pool.map(get_input_addrs, set(df['tx_hash']))
	    pool.close()
	    pool.join()
	end = time.time()
	print(f'Creating the prelim clusters took {round((end - start) / 60, 2)} min.')
	return prelim

def construct_clusters(prelim):
    def pairs(lst):
        i = iter(lst)
        first = prev = item = next(i)
        for item in i:
            yield prev, item
            prev = item
        yield item, first
        
    graph = networkx.Graph()
    for cluster in prelim:
        for edge in pairs(cluster):
            graph.add_edge(*edge)
    clusters = list(networkx.connected_components(graph))
    return clusters
prelim = get_prelim_clusters(df)
clusters = construct_clusters(prelim)


# In[43]:


print(len(prelim))


# In[45]:


print(len(clusters))


# In[13]:


def picklify(obj, filename):
# save obj in a pickle file for later
    with open(filename, 'wb') as file:
        pickle.dump(obj, file)
picklify(clusters, 'clusters.pickle')

def unpicklify(filename):
    with open(filename, 'rb') as file:
        return pickle.load(file)
# clusters = unpicklify('clusters.pickle')


# In[19]:


def clusters_are_consolidated(clusters):
    for idx, cluster in enumerate(clusters):
        for other in clusters[idx + 1:]:
            inter = cluster.intersection(other)
            if inter:
                return False
    return True

# def consolidate_clusters(clusters):
#     consolidated = clusters_are_consolidated(clusters)
    
#     while()


# In[15]:


set(df[df['block_hash'] == '499d9daf3a398f5cfd6ccc060616423a83c16725be71bfa13a1115fd7fc03d85']['tx_hash'])


# In[11]:


set([time[:10] for time in df['block_timestamp']])


# In[13]:


df.columns


# In[26]:


df[df['outputs_addresses']=='LLMRAtr3qBje2ySEa3CnZ55LA4TQMWnRY3']['is_coinbase']

