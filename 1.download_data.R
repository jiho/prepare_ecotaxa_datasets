#
#
# Download taxa from an Ecotaxa Project
#
# (c) 2024 Jean-Olivier Irisson, Flavien Petit and Victoria Bancel, GNU General Public License v3

# Load packages
library("ecotaxarapi")
library("progress")
library("yaml")
library("dplyr")
library("arrow")

# Read config
cfg = read_yaml("config.yaml")
print(cfg$dataset)

# Rrepare storage -----
data_dir <- file.path(path.expand(cfg$base_dir), cfg$dataset)
dir.create(data_dir, showWarnings = FALSE, recursive = TRUE)

# Login to ecotaxa -----
id = cfg$ecotaxa_user
pwd = cfg$ecotaxa_pass

token <- login(LoginReq=LoginReq(password=pwd,username=id))
options(ecotaxa.token_path = file.path(cfg$base_dir, "token.txt"))
save_api_token(token)


# Get validated objects and their metadata from project -----
project <-  cfg$proj_id
project_infos <- project_query(project)

# Fields 
objects_fields <- lapply(names(project_infos$obj_free_cols), function(k) {
  paste0('fre.', k)
})
# Get taxonomic name and image file name and all free fields
fields <- paste(c('txo.id,txo.display_name,img.file_name,obj.latitude,obj.longitude', objects_fields), collapse = ',')

# Only Validated projects
filters <- ProjectFilters(statusfilter = "V")

# Fetch per sample
samples <- samples_search(project, id_pattern = "*")

# Fetch one object to get the total number of objects to fetch
first_obj <- get_object_set(project, ProjectFilters = filters, fields = fields, window_start = 0, window_size = 1)
print(paste0(first_obj$total_ids, " objects to fetch"))

# Progess bar
pbar <- progress_bar$new(
  format = "  downloading [:bar] :percent in :elapsed",
  total = first_obj$total_ids,
  clear = FALSE,
  width = 60
)

# Prepare storage
objs_dfs = data.frame()

for (sam in unique(samples$sampleid)) {
  
  # Update filters to add sampleid
  filters$samples <- as.character(sam)
  
  # Fetch a batch of objects
  objs <- get_object_set(cfg$proj_id, fields = fields, ProjectFilters = filters)
  if (length(objs$details) > 0) {
    n_fetched_objs <- nrow(objs$details)
  
    # Format retrieved data as a DataFrame + add colnames from fields
    objs_df <- as.data.frame(objs$details)
    colnames(objs_df) <- unlist(strsplit(fields, ","))
    # Add object id
    objs_df <- mutate(objs_df, objid = objs$object_ids)
  
   # Store with the previous batches
    objs_dfs <- append(objs_dfs, list(objs_df))
  
    # And update progress bar
    pbar$tick(n_fetched_objs)
  }
}

# Combine all batches in a single DataFrame
df <- do.call(rbind, objs_dfs)

# Fix id column types
df$txo.id <- as.integer(df$txo.id)
df$objid <- as.integer(df$objid)

# Get all unique taxa ids
taxo_ids <- unique(df$txo.id)

# Get lineage for each
taxa <- lapply(taxo_ids, query_taxa)
lineages <- sapply(taxa, function(t) {
  lineage <- rev(t$lineage)
  paste("/", paste(lineage, collapse = "/"), sep = "")
})

# Add lineages to the DataFrame
taxo <- data.frame(lineages, taxo_ids) %>% rename('txo.id' = taxo_ids, 'lineage' = lineages)
df <- df %>%  left_join(taxo, by='txo.id') 

# Compute number of examples per class
inventory <- df %>% group_by(lineage, txo.id, txo.display_name) %>% summarise(n0 = n()) %>%  ungroup() %>%  rename('id' = txo.id, 'level0' = txo.display_name)

print(paste0('Downloaded ', nrow(df), ' objects. Saving them'))

# Write to disk
file_path <- file.path(data_dir, "orig_extraction.parquet")
write_parquet(
  df,
  sink =file_path,
  compression= NULL)

write.csv(inventory, file.path(data_dir, 'orig_inventory.csv'),row.names = FALSE)
