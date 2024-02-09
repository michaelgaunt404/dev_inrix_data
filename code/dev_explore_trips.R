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

data_location = "data/example_data/trips"
specifically = NULL
latest = F

file_list = here(data_location) %>%
  list.files() %>%
  .[str_detect(., "parquet")] %>%
  { if (!is.null(specifically)) (.) %>% .[str_detect(., specifically)] else .} %>%
  { if (latest) .[parse_number(.) == max(parse_number(.))] else .}

file_list = file_list[4]

library(furrr)
plan(multisession, workers = 10)
seed(1234)
temp_time_start = Sys.time()

temp_data = file_list %>%
  furrr::future_map(
    # seed = NULL,
    ~{

      message(str_glue("Processing {.x}"))

      temp_time_start = Sys.time()

      tryCatch({
        temp_data = here(
          data_location
          ,.x
        ) %>%
          arrow::read_parquet()

        start_row = nrow(temp_data)

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

        end_row = nrow(temp_data)

      },  error = function(e) {
        error_message <- paste("An error occurred:\n", e$message)

        return(NA)
      })

      temp_time_duration = round(Sys.time()-temp_time_start, 2)
      temp_removed = 1-round(end_row/start_row, 2)

      message(str_glue("Completed in {temp_time_duration} seconds"))
      message(str_glue("Removed {100*temp_removed}% of trips"))


      return(
        list(
          temp_time_duration = temp_time_duration
          ,removed = temp_removed
          ,trips = temp_data
        )
      )

    })

temp_time_duration = Sys.time()-temp_time_start

gauntlet::alert_me()

temp_data_trip = temp_data
temp_data_trip = temp_data[1,]

#main header====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

temp_data_trip %>% glimpse()
temp_data_trip$zone_names

temp_data_trip$trajectories[[1]][["solution_segments"]][[1]] %>% glimpse()

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










































