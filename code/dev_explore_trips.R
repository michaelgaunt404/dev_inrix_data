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
specifically = NULL
latest = F

file_list = here(data_location) %>%
  list.files() %>%
  .[str_detect(., "parquet")]

#perFile_processing====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
plan(multisession, workers = 6)
# seed(123)
temp_time_start = Sys.time()

file_length = length(file_list)
batch_limit = 200
loop_number = ceiling(file_length/batch_limit)

large_dump_object = list()

for (i in 1:loop_number){
  print(i)

  index_min = ((i-1)*(batch_limit))+1
  index_max = i*batch_limit

  temp_file_list = file_list[index_min:index_max] %>%
    .[!is.na(.)]

  # print((temp_file_list))

  temp_time_start_loop = Sys.time()

  with_progress({
    p <- progressor(steps = batch_limit)

    temp_data = temp_file_list %>%
      # .[c(1:limit)] %>%
      furrr::future_map(
        # seed = NULL,
        ~{

          p()

          # message(str_glue("Processing {.x}"))

          # temp_time_start = Sys.time()

          tryCatch({
            temp_data = here(
              data_location
              ,.x
            ) %>%
              arrow::read_parquet()

            # start_row = nrow(temp_data)

            temp_data = temp_data %>%
              data.table() %>%
              .[,.(trip_id, device_id, provider_id, start_date
                   ,start_lat, start_lon, end_lat, end_lon)] %>%
              .[,`:=`(geometry = paste0("LINESTRING (", start_lon, " ", start_lat, ",  ", end_lon, " ", end_lat, ")") %>%
                        as.character()
              )] %>%
              sf::st_as_sf(wkt = "geometry", crs = 4326) %>%
              sf::st_filter(boundary) %>%
              sf::st_drop_geometry()

            # nrow(temp_data) %>% print()

            # end_row = nrow(temp_data)

          },  error = function(e) {
            error_message <- paste("An error occurred:\n", e$message)

            return(NA)
          })

          # temp_time_duration = round(Sys.time()-temp_time_start, 2)
          # temp_removed = 1-round(end_row/start_row, 2)
          #
          # message(str_glue("Completed in {temp_time_duration} seconds"))
          # message(str_glue("Removed {100*temp_removed}% of trips"))


          return(
            # list(
            # temp_time_duration = temp_time_duration
            # ,removed = temp_removed
            # ,trips =
            temp_data
            # )
          )

        })
  })

  large_dump_object[i] = temp_data

  temp_time_duration_loop = Sys.time()-temp_time_start_loop
  print(temp_time_duration_loop)


}

temp_time_duration_ttl = Sys.time()-temp_time_start

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


#batch_processing====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
plan(multisession, workers = 6)
# seed(123)
temp_time_start = Sys.time()

limit = 100
# limit = length(file_list)

with_progress({
  p <- progressor(steps = limit)

  temp_data = file_list %>%
    .[c(1:limit)] %>%
    furrr::future_map(
      # seed = NULL,
      ~{

        p()

        message(str_glue("Processing {.x}"))

        temp_time_start = Sys.time()

        tryCatch({
          temp_data = here(
            data_location
            ,.x
          ) %>%
            arrow::read_parquet() %>%
            data.table() %>%
            .[,.(trip_id, device_id, provider_id, start_date
                 ,start_lat, start_lon, end_lat, end_lon)]

        },  error = function(e) {
          error_message <- paste("An error occurred:\n", e$message)

          return(NA)
        })


        return(
          temp_data
        )

      })

  temp_data = temp_data %>%
    rbindlist() %>%
    .[,`:=`(geometry = paste0("LINESTRING (", start_lon, " ", start_lat, ",  ", end_lon, " ", end_lat, ")") %>%
              as.character()
    )] %>%
    # sf::st_as_sf(wkt = "geometry", crs = 4326) %>%
    # sf::st_filter(boundary) %>%
    # sf::st_drop_geometry()
})

temp_time_duration = Sys.time()-temp_time_start


##sub header 1==================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


boundary = mapedit::drawFeatures() %>%
  sf::st_transform(4326)

function(trip_file){

  trip_file %>%
  data.table() %>%
  .[,.(trip_id, device_id, provider_id, start_date
         ,start_lat, start_lon, end_lat, end_lon)] %>%
  .[,`:=`(geometry = paste0("LINESTRING (", start_lon, " ", start_lat, ",  ", end_lon, " ", end_lat, ")") %>%
            as.character()
  )] %>%
  sf::st_as_sf(wkt = "geometry", crs = 4326) %>%
  sf::st_filter(boundary) %>%
  sf::st_drop_geometry()

}

##sub header 2==================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

temp_save_list = qs::qread(
  here("data", "texas_spatial_object_list.qs")
)

index_texas_omsids = temp_save_list$index_texas_omsids

#script end=====================================================================










































