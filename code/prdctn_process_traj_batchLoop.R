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
library(polars)



#object import==================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
index_texas_omsids = here(
  "data/gis"
  ,"texas_spatial_object_list_all_entries.qs") %>%
  qs::qread() %>%
  .[['index_texas_omsids']] %>%
  sort()

data_location = "E:/010_projects/trips_usa_tx_202203_wk2/trajs"
# data_location_write = here::here("data/extracted_traj/test_1")
data_location_write = here::here("data/extracted_traj/trips_usa_tx_202203_wk2_part2")

# data_location = "E:/010_projects/trips_usa_tx_202203_wk3/trajs"
# data_location_write = here::here("data/extracted_traj/test_2")
# data_location = "E:/010_projects/trips_usa_tx_202208_wk4/date=2023-11-13/reportId=166942/v1/data/trajs"
# data_location_write = here::here("data/extracted_traj/test_3")
# data_location = "E:/010_projects/trips_usa_tx_202210_wk4/trajs"
# data_location_write = here::here("data/extracted_traj/trips_usa_tx_202210_wk4_part2")


#path set-up====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
limit = NA
cores = 33
batch_limit = 250

files = list.files(data_location, pattern = "par") %>% here::here(data_location, .)
# file_list_use = files[1:ifelse(is.na(limit), length(files), limit)]
file_list_use = files[30001:ifelse(is.na(limit), length(files), limit)]

{
  counter_empty_file = c()
  # check_device_id = c("init")

  loop_number = ceiling(length(file_list_use)/batch_limit)

  temp_time_start_ttl = Sys.time()

  for (i in 1:loop_number){

    index_min = ((i-1)*(batch_limit))+1
    index_max = i*batch_limit

    temp_file_list_use = file_list_use[index_min:index_max] %>% .[!is.na(.)]

    message(stringr::str_glue("Batch {i} of {loop_number} running....\n---- Batch contains {length(temp_file_list_use)} records"))

    temp_time_start_sub = Sys.time()

    plan(multisession, workers = cores)
    {
      temp_time_start = Sys.time()

      with_progress({
        p <- progressor(steps = length(temp_file_list_use))

        temp_extracted_trips =
          temp_file_list_use %>%
          furrr::future_map(
            ~{
              p()

              tryCatch({

                temp_data =
                  arrow::read_parquet(.x) %>%
                  .[, c('trip_id', 'device_id', 'provider_id', 'trajectories'
                        ,'trip_raw_distance_m', 'start_utc_ts', 'end_utc_ts'
                        )
                    ] %>%
                  rename(
                    trip_raw_distance_m_ttl = trip_raw_distance_m
                    ,start_utc_ts_ttl = start_utc_ts
                    ,end_utc_ts_ttl = end_utc_ts
                    ) %>%
                  unnest(cols = trajectories) %>%
                  unnest(cols = solution_segments)

                # temp_data_filtered = temp_data[temp_data$segment_id %in% index_texas_omsids,]
                # print(unique(temp_data_filtered$device_id))
                # check_device_id <-- c(check_device_id, temp_data_filtered$device_id) %>% unique()
                # print(check_device_id)

                temp_data = temp_data %>%
                  .[#temp_data$trip_id %in% unique(temp_data_filtered$trip_id)
                      # temp_data$device_id %in% check_device_id #partially implemented - does not work as future framework to save out devices
                    ,c('trip_id', 'device_id', 'provider_id'
                       ,'trip_raw_distance_m_ttl', 'start_utc_ts_ttl', 'end_utc_ts_ttl'
                       ,'traj_idx', 'segment_id', 'segment_idx'
                       ,"speed_kph"
                       ,"snap_count", "on_road_snap_count", "service_code"
                       )] %>%
                  mutate(file = .x)

              },  error = function(e) {
                error_message <- paste("An error occurred:/n", e$message)

                return(NA)
              })

              return(temp_data)

            }) %>%
          rbindlist() %>%
          .[,`:=`(speed_kph = round(speed_kph, 0)
                  ,trip_raw_distance_m_ttl = round(trip_raw_distance_m_ttl, 0))]

      })

      temp_time_duration_sub = round(Sys.time()-temp_time_start_sub, 3)

      message(str_glue("---- Completed in {temp_time_duration_sub}"))
    }

    counter_empty_file = c(counter_empty_file, nrow(temp_extracted_trips) == 0)

    message(str_glue("---- {100*round(mean(counter_empty_file), 2)}% of processed batches have been empty"))

    stopifnot("Aborting to unusually high number of empty batches" = !(i > 2 & mean(counter_empty_file) > .9))

    if (nrow(temp_extracted_trips) != 0){
      tryCatch({

        temp_extracted_trips %>%
          mutate(process_duration = as.numeric(temp_time_duration_sub)
                 ,batch_id = i) %>%
          arrow::write_parquet(
            here::here(
              data_location_write
              ,str_glue("traj_{limit}_future{cores}core_{gauntlet::strg_clean_datetime()}.parquet")))

      },  error = function(e) {
        error_message <- paste("An error occurred:/n", e$message)
        return(NA)
      })
    }

    rm(temp_extracted_trips)

  }

  temp_time_duration_ttl = round(Sys.time()-temp_time_start_ttl, 3)

}

temp_extracted_trips %>%
  arrow::write_parquet(
    here::here(
      "data"
      ,str_glue("test.parquet")
    )
  )


#path set-up====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# data_location_write = here::here("data/extracted_traj/test_1")
# data_location_write = here::here("data/extracted_traj/test_2")
# data_location_write = here::here("data/extracted_traj/test_3")
data_location_write = here::here("data/extracted_traj/trips_usa_tx_202210_wk4_part2")

sample_data = arrow::open_dataset(
  data_location_write, format = "parquet"
)

sample_data %>%
  select(file, trip_id, process_duration) %>%
  group_by(file) %>%
  summarise(count = n()
            # ,process_duration_mean = mean(as.numeric(process_duration))
            ) %>%
  collect() %>%
  pull(count) %>%
  mean()




sample_data %>%
  head(1000) %>%
  collect()

file = "traj_100_future33core_20240305_112047104101.parquet"
file = "traj_100_future33core_20240305_112506641378.parquet"
file = "traj_100_future33core_20240305_113407157859.parquet"
file = "traj_100_future33core_20240305_115807127168.parquet"
file = "traj_5_future33core_20240306_164800613986.parquet"

sample_data = arrow::read_parquet(
  str_glue("{data_location_write}/{file}")
)

1661215746000        - 1661214555000


sample_data %>%
  pull(segment_id) %>%
  unique() %>%
  clipr::write_clip()




