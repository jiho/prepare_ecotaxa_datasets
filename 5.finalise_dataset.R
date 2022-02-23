#!/usr/bin/Rscript
#
# Generate descriptive info for the dataset and save it
#
# (c) 2022 Jean-Olivier Irisson, GNU General Public License v3

suppressMessages(library("yaml"))
suppressMessages(library("tidyverse", warn.conflicts=FALSE))
suppressMessages(library("arrow", warn.conflicts=FALSE))

# read config
cfg = read_yaml("config.yaml")
message("### Finalise ", cfg$dataset)

# define i/o directory
data_dir = file.path(cfg$base_dir, cfg$dataset)

message("Plot data map") # ----

# read extracted data
df = read_parquet(file.path(data_dir, "orig_extraction.parquet"))

world = read_csv("gshhg_world_c.csv.gz", col_types=cols())

p <- df %>% select(lat=obj.latitude, lon=obj.longitude) %>%
  distinct() %>%
  ggplot(aes(x=lon, y=lat)) +
    coord_quickmap() +
    geom_polygon(data=world, fill="white") +
    geom_point(na.rm=TRUE) +
    scale_x_continuous(expand=c(0,0)) +
    scale_y_continuous(expand=c(0,0))
ggsave(file=file.path(data_dir, "map.png"), plot=p, width=9, heigh=5)

message("Compute taxonomic inventory") # ----

taxa = read_csv(file.path(data_dir, "taxa.csv.gz"), col_types=cols()) %>%
  count(lineage, taxon) %>%
  arrange(lineage)

write_tsv(taxa, file.path(data_dir, "inventory.tsv"))

message("Create archive of dataset") # ----

# go to the local dir, for the paths in the tarfile to make sense
here = getwd()
setwd(cfg$base_dir)

# list files to archive
files = list.files(cfg$dataset, full.names=TRUE) %>%
  str_subset(pattern="orig", negate=TRUE)

# create the archive, from scratch
tar_file = str_c(cfg$dataset, ".tar")
unlink(tar_file)
tar(tar_file, files=files)

setwd(here)
