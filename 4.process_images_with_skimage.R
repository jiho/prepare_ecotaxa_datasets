#!/usr/bin/Rscript
#
# Read taxo grouping and split train/val/test
#
# (c) 2022 Jean-Olivier Irisson, GNU General Public License v3

library("yaml")
library("tidyverse", warn.conflicts=FALSE)
library("arrow")
library("fs")
library("parallel")

# read config
cfg = read_yaml("config.yaml")
message("### Processing ", cfg$dataset, " ###")

# define i/o directory
data_dir = file.path(cfg$base_dir, cfg$dataset)

message("Define images to process") # ----

# read extracted data and the regrouped taxon name
df = left_join(
    read_parquet(file.path(data_dir, "orig_extraction.parquet"), col_select=c("objid", "img.file_name")),
    read_csv(file.path(data_dir, "taxa.csv.gz"), col_select=c("objid","taxon"), col_types=cols()),
    by="objid"
  )

# read any existing features file
skfeatures_file = file.path(data_dir, 'features_skimage.csv.gz')
if (file_exists(skfeatures_file)) {
  features = read_csv(skfeatures_file, col_types=cols())
} else {
  features = data.frame() #NB: not tibble, for compatibility below
}

# extract file extension and compute file paths
df = mutate(df,
  ext=tools::file_ext(df$img.file_name),
  source=file.path(data_dir, "orig_imgs", str_c(objid, ".", ext)),
  dest=file.path(data_dir, "imgs", taxon, str_c(objid, ".", ext)),
)

# test for the existence of all original images
missing_imgs = sum(!file.exists(df$source))
if (missing_imgs > 0) {
  stop(missing_imgs, "are missing. Please run ./2.download_images.py again.")
}

# define which images to process
df = df %>%
  filter(
    # either the processed image is missing
    ! file.exists(dest) |
    # or the measured features are missing
    ! (objid %in% features$objid)
  )
# remove the existing features for those to avoid duplicates
if (nrow(features) > 0) {
  features = filter(features, objid %in% df$objid)
}
message("  ", nrow(df), " images to process")

message("Process images") # ----

# make destination directories
file.path(data_dir, unique(df$taxon)) %>% walk(dir.create, showWarnings=FALSE)

# process images, in parallel
sk = reticulate::import_from_path("lib_skimage")

# split the data set in chunks to be processed
chunk = 5000
n_cores = 40
n = nrow(df)
dfl = split(df, rep(1:ceiling(n/chunk),each=chunk)[1:n])
# and process them in parallel
props = mclapply(dfl, function(x) {sk$process_images(x, cfg)}, mc.cores=n_cores)
# then get the info back
props = do.call(bind_rows, props) %>%
  mutate(objid=df$objid) %>%
  select(objid, everything())
features = bind_rows(features, props) %>%
  arrange(objid)

message("Save data and cleanup") # ----

# save the features
write_csv(features, file=skfeatures_file)

# remove extra images
all_imgs = list.files(file.path(data_dir, "imgs"), full.names=TRUE, recursive=TRUE)
extra_imgs = all_imgs[!all_imgs %in% df$dest]
unlink(extra_imgs)