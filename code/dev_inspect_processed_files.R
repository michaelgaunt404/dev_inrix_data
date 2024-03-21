#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# This is script is used to post-process files and get process diagnostics
#
# By: mike gaunt, michael.gaunt@wsp.com
#
# README: explodes and then saves out traj files
#-------- benchmarks two methods - both using future_map
#-------- one saves as each item is batched
#-------- other saves after processing batch
#
# *please use 80 character margins
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#library set-up=================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#content in this section should be removed if in production - ok for dev
library(tidyverse)
library(furrr)
library(gauntlet)
library(arrow)
library(data.table)
library(here)
library(progressr)
library(polars)

#sources functions in R folder
targets::tar_source("R")

#object import===============================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
get_processed_file_table = function(probe_object, folder_location){
  temp_file_vec = probe_object$process_diagnostic_files$files %>%
    paste0(folder_location, "/", .)

  tictoc::tic()
  temp_file_list = temp_file_vec %>%
    # head(2000) %>%
    arrow::open_dataset(format = "parquet") %>%
    select(file)  %>%
    collect() %>%
    data.table()
  tictoc::toc()

  return(temp_file_list)
}


#aux_objects===============================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
file_load_vector = qs::qread(
  here::here(
    data_location_write_processed
    ,"file_load_vector.qs"
  )
) %>%
  mutate(file = gsub(".*trajs/", "\\1", file))


#get_diagnostics_from_run=======================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#this probes the processed folder
#---output: list of elements detailing what has been processed
#---should probably use one listed below
# folder_location = "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk2"
# probe_result = probe_processed_folder(folder_location = folder_location)


#programmatically_inspect
df_diagnostic = probe_processed_folder_auto(
  folder_root = "//geoatfilpro1/cadd3/inrix_data/processed_data"
  ,sample_size = 500)

#this opens up probe files given a vector
#----useful when inspecting the last couple of probes to be written
#----and by proxy which files were written last
item = inspect_probe_files(
  probe_result
  ,folder_location = folder_location
  ,last = 10
  # ,index_inspect_vector = c(46082:46084)
)

#get/condense_processed_summary_tables==========================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
folder_location = "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk2"

temp_condendesd = condense_processed_summary_tables(
  folder_location = folder_location
  ,cores = 60
)






















tictoc::tic()
processed_files = get_processed_file_table(
  probe_object = probe_result
  ,folder_location = folder_location
)
tictoc::toc()

max_processed = nrow(probe_result$process_diagnostic_files)
last = 60

file_load_vector[(max_processed-last):((max_processed+last)),] %>%
  mutate(flag_processed = case_when(file %in% item$file ~T, T~F))

match(item$file, file_load_vector$file)

probe_result

item %>%
  collect() %>%
  data.table()

file_list_use[index_inspect_vector] %>%
  gsub(".*trajs/", "\\1", .)

