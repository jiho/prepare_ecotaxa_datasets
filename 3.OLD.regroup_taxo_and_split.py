#!/usr/bin/python3
#
# Read taxo grouping and split train/val/test
#
# (c) 2022 Jean-Olivier Irisson, GNU General Public License v3

import os
import yaml
import re

import pandas as pd
from pyarrow.feather import read_feather

from sklearn.model_selection import train_test_split


# read config
with open(r'config.yaml') as config_file:
  cfg = yaml.safe_load(config_file)

# i/o directory
data_dir = os.path.join(os.path.expanduser(cfg['base_dir']), cfg['dataset'])
os.makedirs(data_dir, exist_ok=True)

# read extracted data
df = read_feather(os.path.join(data_dir, 'from_ecotaxa.feather'))

# read taxonomic grouping
url = re.sub('/edit(#|\\?){0,1}', '/export?format=csv&', cfg['grouping_url'])
groups = pd.read_csv(url, index_col='level0')
groups = groups[['level1', 'level2']]

# add taxonomic grouping to extracted data
df = df.join(groups, on='txo.display_name')

# choose level
df = df.rename(columns={cfg['grouping_level'] : 'taxon'})

# recompute lineage
def common_lineage(x):
    # get common prefix
    pre = os.path.commonprefix(x.tolist())
    # remove trailing slash
    pre = re.sub('/$', '', pre)
    return(pre)
lin = df[['taxon', 'lineage']].groupby('taxon').agg(common_lineage)
lin['lineage']

lin.to_csv('lineage.csv', index=True, index_label="taxon")
# recompute lineage

x = [l.split('/') for l in x]
import numpy as np
np.ndarray(x)

dfg = dfg[['objid', 'img_path', 'label']]
# drop images that end up with no taxon name
dfg = dfg.dropna(subset=['label'])


# save to disk
df.to_csv(os.path.join(data_dir), 'taxa.csv.gz')


# split in train-test, stratified by (regrouped) label
train_df,test_df = train_test_split(dfg, test_size=0.1,
    stratify=dfg['label'], random_state=1)

# save to disk
train_df.to_csv('io/training_labels.csv', index=False)
test_df.to_csv('io/unknown_labels.csv', index=False)
