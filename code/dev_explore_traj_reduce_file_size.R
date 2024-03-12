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

index_texas_omsids = c("225591372_0", "435876345_0", "42800598_0", "20773334_0", "42801350_0"
                       , "140641539_0", "492211407_0", "20777051_1", "20765020_0", "525602886_0", "35185641_0"
                       , "145954711_1", "145954694_0"
                       , "365602439_0", "42806640_1", "357815727_0", "461204306_0", "102642059_0")

data_location = "C:/Users/gauntm/Documents/temp/week/trips_usa_tx_202208_wk4/date=2023-11-13/reportId=166942/v1/data/trajs"

file_list = here(data_location) %>%
  list.files(pattern = "parquet")

#main header====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


## loading the files------------------------------------------------------------
x = file_list %>% sample(1)

file = here(data_location, x) %>%
  arrow::read_parquet(
    col_select = c("trip_id", "device_id", "provider_id", "trajectories")
  ) %>%
  data.table()



collec


## extracting sample of traj----------------------------------------------------
# glimpse(file_one)
file_one = file

trajectories = file_one %>%
  .[, .(trip_id, device_id, provider_id, trajectories)]

# trajectories[["trajectories"]][[1]][["solution_segments"]][[1]] %>%
#   map(~{.x["segment_id"]}) %>%
#   reduce(c)


temp = list()
for (i in 1:3){
  trajectories %>%
    .[,.(ids =
           tryCatch({
             trajectories[[1]] %>%
               map(~{.x[["solution_segments"]] %>% print()})
             # reduce(c)
           },  error = function(e) {return(NA_character_)})
    ), by = .(trip_id, device_id, provider_id)]
}




onefile = trajectories[4,]
onefile[["trajectories"]][[1]]


temp = trajectories

zz = temp[1] %>%
  .[,.(ids =
         tryCatch({
           trajectories[[1]][["solution_segments"]] %>%
             map(~{.x[["segment_id"]]}) %>%
             reduce(c)
         },  error = function(e) {return(NA_character_)})

  ), by = .(trip_id, device_id, provider_id)] %>%
  .[ids %in% index_texas_omsids, ]


zz %>%
  arrow::write_parquet(
    "test_file.parquet"

  )

tictoc::tic()
mm = temp[1] %>%
  .[,.(ids =
         tryCatch({
           trajectories[[1]][["solution_segments"]] %>%
             map(~{.x[["segment_id"]]}) %>%
             reduce(c)
         },  error = function(e) {return(NA_character_)})
  ), by = .(trip_id, device_id, provider_id)]
tictoc::toc()

tictoc::tic()
mn = temp[1] %>%
  .[,.(ids =
         tryCatch({
           trajectories[[1]][["solution_segments"]] %>%
             map(~{.x %>%  #print()
                 .[, c( 'segment_id', 'segment_idx')]
             }) %>%
             reduce(bind_rows)
         },  error = function(e) {return(NA_character_)})

  ), by = .(trip_id, device_id, provider_id)]
tictoc::toc()


index_traj_cols = c("segment_id", "segment_idx", "length_m", "start_offset_m"
                    ,"end_offset_m", "start_utc_ts", "end_utc_ts", "solution_snaps"
                    ,"speed_kph", "snap_count", "on_road_snap_count", "error_codes"
                    ,"raw_speed_kph", "source_segment_id", "service_code", "highway_code")

tictoc::tic()
mo =  temp[1, ] %>%
  .[, .(trip_id, device_id, provider_id, trajectories)] %>%
  .[,`:=`(ids =
            tryCatch({
              trajectories[[1]] %>%
                unnest(cols = "solution_segments") #%>%
                # .[,c('traj_idx', 'segment_id', 'segment_idx')]
            },  error = function(e) {return(NA_character_)})

  )] %>%
  unnest(cols = "ids") %>%
  select(!trajectories)
tictoc::toc()



mo %>%
  arrow::write_parquet(
    "test_file_2.parquet"
  )


file_sm =  "test_file.parquet" %>% file.size()/1000
file_sm_1 =  "test_file_1.parquet" %>% file.size()/1000
file_sm_2 =  "test_file_2.parquet" %>% file.size()/1000
file_big = here(data_location, x) %>% file.size()  /1000


(file_sm_2-file_big)/file_big

temp[1,] %>% class()
test[1,] %>% class()


class()





plan(multisession, workers = 6)

temp_time_start = Sys.time()

limit = 100

file_list_sm = file_list[1:2]

with_progress({
  p <- progressor(steps = length(file_list_sm))

  temp_data_rr =
    list(
      file_list_sm
      ,c(1:length(file_list_sm))
    ) %>%
    pmap(
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
            data.table()

          print(head(temp_data))
          print(nrow(temp_data))

          test <<- temp_data

          # temp_data = temp_data %>%
          #   .[,.(ids =
          #          tryCatch({
          #            trajectories[[1]][["solution_segments"]] %>%
          #              map(~{
          #                .x[["segment_id"]]
          #                }) %>%
          #              reduce(c) %>%
          #              print()
          #          },  error = function(e) {return(NA_character_)})
          #   ), by = .(trip_id, device_id, provider_id)]
          #   .[,`:=`(file = x)]

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


end_time_start = Sys.time()-temp_time_start




















