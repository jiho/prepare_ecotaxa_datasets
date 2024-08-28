#
## Download images from EcoTaxa
#
#
#load packages
library("ecotaxarapi")
library("tools")
library('utils')
library(stringr)
library(yaml)
library(arrow)
library(progress)
library(dplyr)



#read config
cfg = read_yaml("config.yaml")

print(paste0('### Download images for ', cfg$dataset))


# prepare storage for orig_imgs
data_dir = file.path((cfg$base_dir), cfg$dataset)
img_dir = file.path(data_dir, 'orig_imgs')
dir.create(img_dir, recursive = TRUE)

# read data from EcoTaxa
df <- read_parquet(file.path(data_dir, 'orig_extraction.parquet'))

# detect image extension
ext = file_ext(df$img.file_name)[1]

# name image according to internal object_id to ensure uniqueness
df$dest_path = file.path(img_dir, paste0(df$objid, '.', ext))

#vault_path exist ?
vault_path <- '/remote/ecotaxa/vault'
vault <- file.exists(vault_path)

# download images (with a nice progress bar)
pbar <- progress_bar$new(
  format = "  downloading [:bar] :percent in :elapsed",
  total = nrow(df),
  clear = FALSE,
  width = 60
)

for (i in 1:nrow(df)){ 
  if (!file.exists(df$dest_path[i])){ #if the file has not been copied already
      if (vault) {
        res <- file.copy(
          from = file.path(vault_path, df$path_to_img[i]),
          to =df$dest_path[i])
      }else {
        res <- download.file(
          url= paste0('https://ecotaxa.obs-vlfr.fr/vault/',df$img.file_name[i]),
          destfile = df$dest_path[i],
          quiet = TRUE
        )
      }
    }
  pbar$tick(1)

}

n_imgs <- length(list.files(img_dir))
print(paste0(n_imgs,' images in ', img_dir))

