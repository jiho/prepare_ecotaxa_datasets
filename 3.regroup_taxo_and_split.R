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
message("### Processing ", cfg$dataset, " ###")

# define i/o directory
data_dir = file.path(cfg$base_dir, cfg$dataset)

message("Define taxo grouping") # ----

# read the taxo inventory
taxo = read_tsv(file.path(data_dir, "orig_inventory.tsv"), col_types=cols())

# read taxonomic grouping
file_id = scan(str_c("taxo/taxo_", str_to_lower(cfg$dataset), ".gdsheet"), what="character", quiet=TRUE)[6]
url = str_c("https://docs.google.com/spreadsheets/d/", file_id, "/export?format=csv")
groups = read_csv(url, col_types=cols())

# merge the grouping with the (potentially new) inventory
new_groups = left_join(taxo, select(groups, -nb), by=c("lineage", "id", "level0"))
write_tsv(
  new_groups,
  file=str_c("taxo/taxo_", str_to_lower(cfg$dataset), "_base.tsv"),
  na=""
)

## Now integrate the .tsv file in the gdsheet and define the groups
## If this is done, the process can continue

message("Regroup taxa") # ----

# read extracted data
df = read_parquet(file.path(data_dir, "orig_extraction.parquet"))

# add taxonomic grouping to extracted data
groups = groups %>%
  select(id, num_range("level", range=0:5, width=1))
df = left_join(df, groups, by=c("txo.id"="id"))

# choose grouping level
df$taxon = df[[cfg$grouping$level]]
n_before = nrow(df)
# remove objects which are not assigned at that level
# (ideally there should be very little to none)
df = filter(df, !is.na(taxon))
n_after = nrow(df)
message("  removed ", n_before - n_after, " objects that were not assigned to a taxon")

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

message("Split into train, val, and test") # ----

# split data into train, val, and test
set.seed(1)
df = df %>%
  arrange(objid) %>%
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
  select(-any_of(cfg$images$useless_features)) %>%
  write_csv(file.path(data_dir, "features_native.csv.gz"))
