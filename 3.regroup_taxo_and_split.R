#!/usr/bin/Rscript
#
# Read taxo grouping and split train/val/test
#
# (c) 2022 Jean-Olivier Irisson, GNU General Public License v3

library("yaml")
library("tidyverse", warn.conflicts=FALSE)
library("arrow")
library("fs")

# read config
cfg = read_yaml("config.yaml")

# define i/o directory
data_dir = file.path(cfg$base_dir, cfg$dataset)

# read extracted data
df = read_parquet(file.path(data_dir, "orig_extraction.parquet"))

# read taxonomic grouping
url = str_replace(cfg$grouping$url, "/edit(#|\\?){0,1}", "/export?format=csv&")
groups = read_csv(url, col_types=cols()) %>%
  select(num_range("level", range=0:3, width=1))

# add taxonomic grouping to extracted data
df = left_join(df, groups, by=c("txo.display_name"="level0"))

# choose grouping level
df$taxon = df[[cfg$grouping$level]]

# recompute lineage to match the grouping level
new_lineages = df %>%
  group_by(taxon) %>%
  # find common path to current taxon
  summarise(lineage=path_common(unique(lineage))) %>%
  # if lineage does not match taxon, go one up and add the taxon name
  mutate(lineage=ifelse(
    path_file(lineage) == taxon,
    lineage,
    str_c(path_dir(lineage), "/", taxon)
  ))

# clean up taxonomy-related columns
df = df %>%
  select(-starts_with("txo"), -lineage, -num_range("level", range=0:3, width=1)) %>%
  left_join(new_lineages, by="taxon") %>%
  drop_na(taxon)


# split data into train, val, and test
set.seed(1)
df = df %>%
  group_by(taxon) %>%
  mutate(set=sample(c(
    rep("val", times=n()*cfg$split_props$val),
    rep("test", times=n()*cfg$split_props$test),
    rep("train", times=n())
  )[1:n()])) %>%
  ungroup()


# write to disk
df %>%
  select(objid, taxon, lineage, set) %>%
  write_csv(file.path(data_dir, "taxa.csv.gz"))

df %>%
  select(objid, starts_with("fre.")) %>%
  rename_with(~str_replace(., fixed("fre."), "")) %>%
  select(-all_of(cfg$images$useless_features)) %>%
  write_csv(file.path(data_dir, "features_native.csv.gz"))