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
library(tidyverse)
library(gauntlet)
library(arrow)
library(data.table)
library(here)

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

temp_save_list = qs::qread(
  here("data", "texas_spatial_object_list.qs")
)

index_texas_omsids = temp_save_list$index_texas_omsids

data_location = here("data/example_data/trajs")


traj_ex = file_list = here(data_location) %>%
  list.files() %>%
  .[str_detect(., "parquet")] #%>%
  # { if (!is.null(specifically)) (.) %>% .[str_detect(., specifically)] else .} %>%
  # { if (latest) .[parse_number(.) == max(parse_number(.))] else .}


#main header====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

traj_ex_trip = traj_ex[1:3,]

glimpse(traj_ex_trip)

trajectory = traj_ex_trip$trajectories[[1]]

glimpse(trajectory)

library(furrr)
plan(multisession, workers = 10)
# seed(1234)
temp_time_start = Sys.time()

temp_data = file_list[c(1, 3)] %>%
  furrr::future_map(
    ~{
      message(str_glue("Processing {.x}"))

      tryCatch({
        temp_data = here(
          data_location
          ,.x
        ) %>%
          arrow::read_parquet() %>%
          data.table() %>%
          .[, .(trip_id, device_id, provider_id, trajectories)]

      },  error = function(e) {
        error_message <- paste("An error occurred:\n", e$message)

        return(NA)
      })

      return(
        temp_data
      )

    }) %>%
  rbindlist()

temp_time_duration = Sys.time()-temp_time_start


tmp = temp_data[[20]]$trips


tt[1, c('trip_id', 'device_id', 'provider_id', 'trajectories')][["trajectories"]][[1]][["solution_segments"]] %>%
  map(~{.x[["segment_id"]] }) %>%
  reduce(c)
tt[2, c('trip_id', 'device_id', 'provider_id', 'trajectories')][["trajectories"]]
tt[3, c('trip_id', 'device_id', 'provider_id', 'trajectories')][["trajectories"]]

tryCatch({
  tt[4, c('trip_id', 'device_id', 'provider_id', 'trajectories')][["trajectories"]][[1]][["solution_segments"]] %>%
    map(~{.x[["segment_id"]] }) %>%
    reduce(c)
},  error = function(e) {return(NA)})
tt[5, c('trip_id', 'device_id', 'provider_id', 'trajectories')][["trajectories"]][[1]] %>%
  map(~{.x[["segment_id"]] }) %>%
  reduce(c)
tt[1018, c('trip_id', 'device_id', 'provider_id', 'trajectories')][["trajectories"]][[1]][["solution_segments"]] %>%
  # map(dim)
  map(~{.x[["segment_id"]] }) %>%
  reduce(c)


tt = temp_data %>%
  .[,.(ids =

         tryCatch({
           # tt[4, c('trip_id', 'device_id', 'provider_id', 'trajectories')][["trajectories"]]
           trajectories[[1]][["solution_segments"]] %>%
             map(~{.x[["segment_id"]] }) %>%
             reduce(c)
         },  error = function(e) {return(NA_character_)})

  )
  ,by = .(trip_id, device_id, provider_id)] %>%
  .[ids %in% index_texas_omsids, ]


test = tt$trajectories %>%
  map(~{
    .x[["solution_segments"]][[1]][["segment_id"]]
    # .x[["solution_segments"]]
  })

test[[1]] %in% index_texas_omsids
##sub header 1==================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

##method_1: batch processing====================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
temp_time_start = Sys.time()

temp_data = file_list %>%
  furrr::future_map(
    ~{
      message(str_glue("Processing {.x}"))

      tryCatch({
        temp_data = here(
          data_location
          ,.x
        ) %>%
          arrow::read_parquet() %>%
          data.table() %>%
          .[, .(trip_id, device_id, provider_id, trajectories)]

      },  error = function(e) {
        error_message <- paste("An error occurred:\n", e$message)

        return(NA)
      })

      return(
        temp_data
      )

    }) %>%
  rbindlist()

tt = temp_data %>%
  .[,.(ids =

         tryCatch({
           # tt[4, c('trip_id', 'device_id', 'provider_id', 'trajectories')][["trajectories"]]
           trajectories[[1]][["solution_segments"]] %>%
             map(~{.x[["segment_id"]] }) %>%
             reduce(c)
         },  error = function(e) {return(NA_character_)})

  )
  ,by = .(trip_id, device_id, provider_id)] %>%
  .[ids %in% index_texas_omsids, ]

temp_time_duration_batch = Sys.time()-temp_time_start



##method_2: per file processing=================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
temp_time_start = Sys.time()

temp_data = file_list %>%
  .[c(1:10)] %>%
  furrr::future_map(
    ~{
      message(str_glue("Processing {.x}"))

      tryCatch({
        temp_data = here(
          data_location
          ,.x
        ) %>%
          arrow::read_parquet() %>%
          data.table() %>%
          .[, .(trip_id, device_id, provider_id, trajectories)] %>%
          .[,.(ids =

                 tryCatch({
                   # tt[4, c('trip_id', 'device_id', 'provider_id', 'trajectories')][["trajectories"]]
                   trajectories[[1]][["solution_segments"]] %>%
                     map(~{.x[["segment_id"]] }) %>%
                     reduce(c)
                 },  error = function(e) {return(NA_character_)})

          )
          ,by = .(trip_id, device_id, provider_id)] %>%
          .[ids %in% index_texas_omsids, ]

      },  error = function(e) {
        error_message <- paste("An error occurred:\n", e$message)

        return(NA)
      })

      return(
        temp_data
      )

    }, .progress = T) %>%
  rbindlist()


temp_time_duration_perFile = Sys.time()-temp_time_start

message(temp_time_duration_perFile)

#script end=====================================================================










































