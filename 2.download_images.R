#
## Download images from EcoTaxa
#
#
#load packages
library("ecotaxarapi")
library("tools")
library('utils')
library("stringr")
library("yaml")
library("arrow")
library("progress")
library("dplyr")
library("future.apply")
library("parallel")



# Read config file
cfg <- read_yaml("config.yaml")
print(paste0('### Download images for ', cfg$dataset))


# Prepare storage for orig_imgs
data_dir <- file.path((cfg$base_dir), cfg$dataset)
img_dir <- file.path(data_dir, 'orig_imgs')
dir.create(img_dir, recursive = TRUE)

# Read data from EcoTaxa
df <- read_parquet(file.path(data_dir, 'orig_extraction.parquet'))

# Detect image extension
ext <- file_ext(df$img.file_name)[1]

# Name image according to internal object_id to ensure uniqueness
df$dest_path = file.path(img_dir, paste0(df$objid, '.', ext))

# Check files existence 
file_exists_list <- mclapply(df$dest_path, file.exists, mc.cores = detectCores()/4)
df$file_exists <- file_exists_list

# Filter missing files
df_missing <- df %>% filter(file_exists == FALSE)

# Check vault path existence
vault_path <- '/remote/ecotaxa/vault'
vault <- file.exists(vault_path)

# Function to download/copy files
process_file <- function(row_i, dfmissing, vault) {
  system(paste("echo 'now processing row:",row_i,"' on ", nrow(dfmissing), "row"))
  if (vault) {
    file.copy(
      from = file.path(vault_path, dfmissing$img.file_name[row_i]),
      to = dfmissing$dest_path[row_i]
    )
  } else {
    download.file(
      url = paste0('https://ecotaxa.obs-vlfr.fr/vault/', dfmissing$img.file_name[row_i]),
      destfile = dfmissing$dest_path[row_i],
      quiet = TRUE
    )
  }
}

# Apply fonction in parallel
print("Copying or downloading images, be patient")
mclapply(1:nrow(df_missing), process_file, dfmissing = df_missing, vault = vault, mc.cores = detectCores()/4)

# Check images in directory
n_imgs <- length(list.files(img_dir))
print(paste0(n_imgs, ' images in ', img_dir))

