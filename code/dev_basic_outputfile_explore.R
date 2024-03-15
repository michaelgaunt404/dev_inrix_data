#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# This is script benchmarks time to process and save out traj data
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

#raw_files_from_inrix===========================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#trips_example
file = "//geoatfilpro1/cadd3/inrix_data/trips_usa_tx_202202_wk2/date=2023-11-16/reportId=167124/v1/data/trips/part-00000-42810778-5dd1-4dd7-bd77-62af98897d92-c000.gz.parquet"
df = arrow::read_parquet(file)

#out_put_files==================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#trips_example
file_processed_trips = "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk2/processed_trips_usa_tx_202202_wk2_20240314_031750049141.parquet"
file_summary = "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk2/summary_data_trips_usa_tx_202202_wk2_20240314_0317386675.parquet"
file_process_diagnostic = "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk2/process_diagnostic_trips_usa_tx_202202_wk2_20240314_020004481172.parquet"

df = arrow::read_parquet(file_processed_trips)
df = arrow::read_parquet(file_summary)
df = arrow::read_parquet(file_process_diagnostic)















