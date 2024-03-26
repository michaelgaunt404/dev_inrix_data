#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# This is script is used to process traj files
#
# By: mike gaunt, michael.gaunt@wsp.com
#
# README: uses stable "process_trajectory_files()" function to process
#--------
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

#aux object import==============================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
texas_border_locations_object = qs::qread(
  here::here(
    "//geoatfilpro1/cadd3/inrix_data/gis"
    ,"extracted_border_crossing_locations_20240318.qs"))

index_texas_omsids = texas_border_locations_object %>%
  .[["border_locations_comb_pro"]] %>%
  .[,"seg_id"]

index_texas_omsids_pro = c(
  index_texas_omsids
  ,paste0("-", index_texas_omsids))

#set variable===================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#specify read locations
# data_location = "//10.120.118.10/cadd3/inrix_data/trips_usa_tx_202202_wk2/date=2023-11-16/reportId=167124/v1/data/trajs"
# data_location = "//geoatfilpro1/cadd3/inrix_data/trips_usa_tx_202208_wk1/date=2023-11-13/reportId=166939/v1/data/trajs"
# data_location = "//geoatfilpro1/cadd3/inrix_data/trips_usa_tx_202208_wk2/date=2023-11-13/reportId=166940/v1/data/trajs"
# data_location = "//geoatfilpro1/cadd3/inrix_data/trips_usa_tx_202208_wk3/date=2023-11-13/reportId=166941/v1/data/trajs"

#this may change sometimes
folder_suffix = "_testmg"

#these imnputs should not change that much
{
  data_location_write_root_folder = "//10.120.118.10/cadd3/inrix_data/processed_data/" #this should be root location - subfolders will be made
  osm_match_links = index_texas_omsids_pro
  number_of_batches = 1
  batch_size = 100
  cores = 60
}

#process taj files==============================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
process_trajectory_files(
  data_location = data_location
  ,folder_suffix = folder_suffix
  ,osm_match_links = index_texas_omsids_pro
)

#process outstanding taj files==============================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
data_location = "//geoatfilpro1/cadd3/inrix_data/trips_usa_tx_202208_wk1/date=2023-11-13/reportId=166939/v1/data/trajs"


df_out_standing = df_diagnostic %>%
  select(files:pct_prcssed) %>%
  unique() %>%
  filter(pct_prcssed < .90)

index = df_out_standing[1][["files"]]

data_location_root = "//geoatfilpro1/cadd3/inrix_data/"

temp_folder = here::here(
  data_location_root
  ,index) %>%
  list.files(full.names = T, recursive = T, pattern = "traj", include.dirs = T)

process_trajectory_files_outstanding(
  data_location = temp_folder
  ,data_location_write_root_folder = data_location_write_root_folder
  ,folder_suffix = folder_suffix
  ,osm_match_links = index_texas_omsids_pro
)






















