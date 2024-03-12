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
library(furrr)
library(gauntlet)
library(arrow)
library(data.table)
library(here)
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

# temp_save_list = qs::qread(
#   here("data", "texas_spatial_object_list.qs")
# )

# index_texas_omsids = temp_save_list$index_texas_omsids

index_texas_omsids = c("225591372_0", "435876345_0", "42800598_0", "20773334_0", "42801350_0"
, "140641539_0", "492211407_0", "20777051_1", "20765020_0", "525602886_0", "35185641_0"
, "145954711_1", "145954694_0"
, "365602439_0", "42806640_1", "357815727_0", "461204306_0", "102642059_0")

data_location = "C:/Users/gauntm/Documents/temp/week/trips_usa_tx_202208_wk4/date=2023-11-13/reportId=166942/v1/data/trajs"

file_list = here(data_location) %>%
  list.files() %>%
  .[str_detect(., "parquet")]

#main header====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

traj_ex_trip = traj_ex[1:3,]

glimpse(traj_ex_trip)

trajectory = traj_ex_trip$trajectories[[1]]

glimpse(trajectory)

library(furrr)

plan(multisession, workers = 6)
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
        error_message <- paste("An error occurred:/n", e$message)

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
  .[c(1:100)] %>%
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

        message("Done")

      },  error = function(e) {
        error_message <- paste("An error occurred:/n", e$message)

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
plan(multisession, workers = 6)


index_reduced_trips = qs::qread(
  here::here("data", "temp_extracted_trips_big.qs")
) %>%
  .$large_dump_object %>%
  rbindlist() %>%
  .$trip_id %>%
  unique()


temp_time_start = Sys.time()

limit = 100

x = file_list[1]

with_progress({
  p <- progressor(steps = limit)

  temp_data_rr =
    list(
      file_list[c(1:limit)]
      ,c(1:length(file_list))[c(1:limit)]
    ) %>%
    furrr::future_pmap(
      ~{
        p()

        message(str_glue("Processing {.x} -- {.y}"))

        tryCatch({
          temp_data = here(
            data_location
            ,x
          ) %>%
            arrow::read_parquet(
              col_select = c("trip_id", "device_id", "provider_id", "trajectories")
            ) %>%
            data.table() %>%
            # .[trip_id %in% index_reduced_trips] %>% #prefiltering_Step
            # .[, .(trip_id, device_id, provider_id, trajectories)] %>%
            .[,.(ids =

                   tryCatch({
                     trajectories[[1]][["solution_segments"]] %>%
                       map(~{.x[["segment_id"]] }) %>%
                       reduce(c)
                   },  error = function(e) {return(NA_character_)})

            )
            ,by = .(trip_id, device_id, provider_id)] %>%
            .[ids %in% index_texas_omsids, ] %>%
            .[,`:=`(file = .x)]

          message("Done")


        },  error = function(e) {
          error_message <- paste("An error occurred:/n", e$message)

          return(NA)
        })

        return(
          temp_data
        )

      }, .progress = F) %>%
    rbindlist()
})

temp_time_duration_perFile_nofilter = Sys.time()-temp_time_start

list(
  temp_extracted_trips.qs = temp_data
  ,temp_time_duration_perFile_nofilter = temp_time_duration_perFile_nofilter
) %>%
  qs::qsave(
    here::here("data", "temp_extracted_trajs_big.qs")
  )

temp_extracted_trips = qs::qread(
  here::here("data", "temp_extracted_trips.qs")
)

message(temp_time_duration_perFile)

#script end=====================================================================


temp_extracted_trajs_big.qs = qs::qread(
  here::here("data", "temp_extracted_trajs_big.qs")
)







































