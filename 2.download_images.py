#!/usr/bin/python3
#
# Download images from EcoTaxa
#
# (c) 2022 Jean-Olivier Irisson, GNU General Public License v3

import os
import yaml

import shutil
import urllib.request

from tqdm import tqdm
from pyarrow.parquet import read_table

# read config
with open(r'config.yaml') as config_file:
    cfg = yaml.safe_load(config_file)
print('### Download images for {}'.format(cfg['dataset']))

# prepare storage
data_dir = os.path.join(os.path.expanduser(cfg['base_dir']), cfg['dataset'])
img_dir = os.path.join(data_dir, 'orig_imgs')
os.makedirs(img_dir, exist_ok=True)

# read data from EcoTaxa
df = read_table(os.path.join(data_dir, 'orig_extraction.parquet')).to_pandas()

# detect image extension
file,ext = os.path.splitext(df['img.file_name'][0])
# TODO extract file extension for each file, like in step 4.
# name image according to internal object_id to ensure uniqueness
df['dest_path'] = [os.path.join(img_dir, str(this_id)+ext) for this_id in df['objid']]

# download images (with a nice progress bar)
vault_path = '/remote/ecotaxa/vault'
for i in tqdm(range(df.shape[0])):
  # if the file has not been copied already
  if not os.path.isfile(df['dest_path'][i]):
    # copy from vault
    if os.path.isdir(vault_path):
      res = shutil.copyfile(
        src=os.path.join(vault_path, df['img.file_name'][i]),
        dst=df['dest_path'][i]
      )
    # or copy through the internet
    else:
      res = urllib.request.urlretrieve(
        url='https://ecotaxa.obs-vlfr.fr/vault/'+df['img.file_name'][i],
        filename=df['dest_path'][i]
      )

n_imgs = len(os.listdir(img_dir))
print('  {} images in {}'.format(n_imgs, img_dir))
