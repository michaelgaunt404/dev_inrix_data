#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# This is script explores spatial object for facility locations.
#
# By: mike gaunt, michael.gaunt@wsp.com
#
# README: from sebastian
#-------- CSV of spatial object
#-------- for freight facilities
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
library(sf)

#raw_files_from_inrix===========================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#trips_example
folder = "//geoatfilpro1/cadd3/inrix_data/gis"
file = "Texas_Emp2023_freight.csv"

df = data.table::fread(
  here::here(
    folder
    ,file
  )
)

#out_put_files==================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#trips_example
file_processed_trips = "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk2/processed_trips_usa_tx_202202_wk2_20240314_031750049141.parquet"
file_summary = "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk2/summary_data_trips_usa_tx_202202_wk2_20240314_0317386675.parquet"
file_process_diagnostic = "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk2/process_diagnostic_trips_usa_tx_202202_wk2_20240314_020004481172.parquet"

df = arrow::read_parquet(file_processed_trips)
df = arrow::read_parquet(file_summary)
df = arrow::read_parquet(file_process_diagnostic)















