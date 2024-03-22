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

#get/condense_processed_summary_tables==========================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

border_xing_table = qs::qread(here::here("//geoatfilpro1/cadd3/inrix_data/gis/", "table_extracted_border_crossing_locations_20240318.qs"))
folder_root = "//geoatfilpro1/cadd3/inrix_data/processed_data"
cores = 60


condense_processed_summary_tables_auto(
  border_xing_table = qs::qread(here::here("//geoatfilpro1/cadd3/inrix_data/gis/", "table_extracted_border_crossing_locations_20240318.qs"))
  ,folder_root = "//geoatfilpro1/cadd3/inrix_data/processed_data"
  ,cores = 60
)




