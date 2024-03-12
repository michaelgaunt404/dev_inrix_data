#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# This is script [[insert brief readme here]]
#
# By: mike gaunt, michael.gaunt@wsp.com
#
# README: [[insert brief readme here]]
#-------- [[insert brief readme here]]
#
# *please use 80 character margins
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#library set-up=================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#content in this section should be removed if in production - ok for dev

library(arrow)
library(data.table)
library(gauntlet)
library(here)
library(tidyverse)
library(progressr)

#path set-up====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#content in this section should be removed if in production - ok for dev

#source helpers/utilities=======================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#content in this section should be removed if in production - ok for dev

#source data====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#content in this section should be removed if in production - ok for dev
#area to upload data with and to perform initial munging
#please add test data here so that others may use/unit test these scripts

boundary = mapedit::drawFeatures() %>%
  sf::st_transform(4326)

data_location = "C:/Users/gauntm/Documents/temp/week/trips_usa_tx_202208_wk4/date=2023-11-13/reportId=166942/v1/data/trips"

#perFile_processing====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
plan(multisession, workers = 6)
# seed(123)
temp_time_start = Sys.time()

file_list = file_list[1:100]

test = process_trips_by_spatial_boundary(
  boundary = boundary
  ,batch_limit = 250
  ,data_location = data_location
  ,skip_file_reduction = F
)

process_trips_by_spatial_boundary = function(data_location
         ,batch_limit = 200
         ,boundary
         ,skip_file_reduction = T){

  gauntlet::check_dir_path(data_location)

  file_list = here::here(data_location) %>%
    list.files() %>%
    .[stringr::str_detect(., "parquet")]
  file_length = length(file_list)

  if (!skip_file_reduction){
    message(str_glue("There are {file_length} records in the user supplied data location"))
    check_reduce = gauntlet::robust_prompt_used("reduce the number of files process")

    if (check_reduce){
      check_num_reduce = T
      while (check_num_reduce){
        reduced = as.numeric(readline("Please provide a number of files to process: "))
        check_num_reduce = !(is.numeric(reduced) & !is.na(reduced))
      }
      message("Proper numeric input received.... ")
      file_list = file_list[1:reduced]
      file_length = length(file_list)

    }
  }

  loop_number = ceiling(file_length/batch_limit)
  message(stringr::str_glue("Trips will be processed in {batch_limit} file batches\n{file_length} records will be processed in {loop_number} batches"))
  temp_time_start = Sys.time()
  message(stringr::str_glue("Commencing trip processing run at {temp_time_start} {gauntlet::strg_make_space_2(last = F)}"))

  large_dump_object = list()

  for (i in 1:loop_number){
    message(stringr::str_glue("Batch {i} of {loop_number} running...."))

    index_min = ((i-1)*(batch_limit))+1
    index_max = i*batch_limit

    temp_file_list = file_list[index_min:index_max] %>%
      .[!is.na(.)]

    temp_time_start_loop = Sys.time()

    with_progress({

      #define progress object which displays function progress
      p <- progressr::progressor(steps = batch_limit)

      temp_data = temp_file_list %>%
        furrr::future_map(
          ~{

            #progess bar function
            p()

            tryCatch({

              #read data
              temp_data = here(
                data_location
                ,.x
              ) %>%
                arrow::read_parquet(
                  # col_select = c('trip_id', 'device_id', 'provider_id', 'start_date'
                  #               ,'start_lat', 'start_lon', 'end_lat', 'end_lon')
                ) %>%
                print()
                # data.table::data.table() %>%
                # .[,`:=`(geometry = paste0("LINESTRING (", start_lon, " ", start_lat, ",  ", end_lon, " ", end_lat, ")") %>%
                #           as.character()
                # )] %>%
                # sf::st_as_sf(wkt = "geometry", crs = 4326) %>%
                # sf::st_filter(boundary) %>%
                # sf::st_drop_geometry()

              print("inside")

            },  error = function(e) {
              error_message <- paste("An error occurred:\n", e$message)

              return(NA)
            })
            return(temp_data)
          })
    })

    print("outside")


    large_dump_object[i] = temp_data
    temp_time_duration_loop = Sys.time()-temp_time_start_loop
    message(stringr::str_glue("Batch completed in {round(temp_time_duration_loop, 2)} {units(temp_time_duration_loop)}"))
  }

  temp_time_duration_ttl = Sys.time()-temp_time_start
  message(stringr::str_glue("Total processing completed in {round(temp_time_duration_ttl, 2)} {units(temp_time_duration_ttl)}"))

  return(large_dump_object)
}






index_reduced_trips = large_dump_object %>%
  rbindlist() %>%
  data.table() %>%
  .[["trip_id"]] %>%
  unique()

list(
  large_dump_object = large_dump_object
) %>%
  qs::qsave(
    here::here("data", "temp_extracted_trips_big.qs")
  )

