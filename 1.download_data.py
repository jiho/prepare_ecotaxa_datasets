#!/usr/bin/python3
#
# Download data from an EcoTaxa project
#
# (c) 2022 Jean-Olivier Irisson, GNU General Public License v3

import os
import yaml

from tqdm import tqdm
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq

import ecotaxa_py_client

from ecotaxa_py_client.api import authentification_api
from ecotaxa_py_client.model.login_req import LoginReq

from ecotaxa_py_client.api import objects_api

from ecotaxa_py_client.api import projects_api
from ecotaxa_py_client.model.project_filters import ProjectFilters

from ecotaxa_py_client.api import samples_api

from ecotaxa_py_client.api import taxonomy_tree_api
from ecotaxa_py_client.model.taxon_model import TaxonModel

# read config
with open(r'config.yaml') as config_file:
    cfg = yaml.safe_load(config_file)

# prepare storage
data_dir = os.path.join(os.path.expanduser(cfg['base_dir']), cfg['dataset'])
os.makedirs(data_dir, exist_ok=True)

# authenticate in EcoTaxa
with ecotaxa_py_client.ApiClient() as client:
    api = authentification_api.AuthentificationApi(client)
    token = api.login(LoginReq(
      username=cfg['ecotaxa_user'],
      password=cfg['ecotaxa_pass']
    ))

config = ecotaxa_py_client.Configuration(
    access_token=token, discard_unknown_keys=True)

# get validated objects and their metadata from project
with ecotaxa_py_client.ApiClient(config) as client:
    # list free fields for project
    projects_instance = projects_api.ProjectsApi(client)
    proj = projects_instance.project_query(cfg['proj_id'])
    obj_fields = ['fre.'+k for k in proj['obj_free_cols'].keys()]
    # TODO support several projects
    
    # get objects
    objects_instance = objects_api.ObjectsApi(client)
    # only validated
    filters = ProjectFilters(statusfilter="V")
    # get taxonomic name and image file name and all free fields
    fields = 'txo.id,txo.display_name,img.file_name,' + ','.join(obj_fields)
    
    # fetch one object to get the total number of objects to fetch
    objs = objects_instance.get_object_set(cfg['proj_id'], filters,
      fields=fields, window_start=0, window_size=1)

    # fetch per sample
    samples_instance = samples_api.SamplesApi(client)
    samples = samples_instance.samples_search(
      project_ids=str(cfg['proj_id']),
      id_pattern="%"
    )
    
    # prepare storage
    objs_dfs = []

    with tqdm(total=objs['total_ids']) as pbar:
        for sam in samples:
            # update filters to add sampleid
            filters.samples = str(sam['sampleid'])
            
            # fetch a batch of objects
            objs = objects_instance.get_object_set(cfg['proj_id'], filters,
              fields=fields)
            n_fetched_objs = len(objs.details)
            
            # format retrieved data as a DataFrame
            objs_df = pd.DataFrame(objs['details'], columns=fields.split(','))
            # add object id as an identifier
            objs_df['objid'] = objs['object_ids']
            
            # store with the previous batches
            objs_dfs.append(objs_df)
            # and update progress bar
            ok = pbar.update(n_fetched_objs)

# combine all batches in a single DataFrame
df = pd.concat(objs_dfs, ignore_index=True)
df['txo.id'] = df['txo.id'].astype('int32')

# get all unique taxa ids
taxo_ids = list(set(df['txo.id']))
# get lineage for each
with ecotaxa_py_client.ApiClient(config) as client:
    taxo_instance = taxonomy_tree_api.TaxonomyTreeApi(client)
    taxa = [taxo_instance.query_taxa(t) for t in taxo_ids]

lineages = ['/' + '/'.join(t['lineage'][::-1]) for t in taxa]

# add lineages to the DataFrame
taxo = pd.DataFrame({'lineage': lineages}, index=taxo_ids)
df = df.join(taxo, on='txo.id')

# compute number of examples per class
inventory = df.groupby(['lineage', 'txo.id', 'txo.display_name']).size().reset_index(name='nb')
inventory = inventory.rename(columns={'txo.id':'id', 'txo.display_name':'level0'})


# write to disk
pq.write_table(
  pa.Table.from_pandas(df),
  where=os.path.join(data_dir, 'orig_extraction.parquet'),
  compression='NONE' # for compatibility with R
)

inventory.to_csv(os.path.join(data_dir, 'orig_inventory.tsv'), sep='\t', index=False)
