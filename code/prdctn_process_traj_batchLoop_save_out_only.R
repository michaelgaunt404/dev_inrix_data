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

#test_1

temp_time_start_sub = Sys.time()

temp_extracted_trips =
  temp_file_list_use %>%
  furrr::future_map(
    ~{
      p()

      temp_extract =  arrow::read_parquet(.x)

      temp_pro = temp_extract %>%
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
        unnest(cols = solution_segments)  %>%
        .[,c('trip_id', 'device_id', 'provider_id'
             # ,'trip_raw_distance_m_ttl', 'start_utc_ts_ttl', 'end_utc_ts_ttl'
             ,'traj_idx', 'segment_id', 'segment_idx'
             ,"speed_kph"
             # ,"snap_count", "on_road_snap_count", "service_code"
        )] %>%
        mutate(
          speed_kph = round(speed_kph, 0)
          ,flag_border_link = case_when(
            segment_id %in% index_texas_omsids ~ 1
            ,T~0
          )) %>%
        group_by(trip_id) %>%
        mutate(flag_ttl = case_when(
          sum(flag_border_link)>0~T
          ,T~F
        )) %>%
        ungroup()

      arrow::write_parquet(
        temp_pro
        ,here::here(
          "data/benchmark_save_out"
          ,str_glue("{gauntlet::strg_clean_datetime()}.parquet")
        )
      )

    })


temp_time_duration_sub_saveout = round(Sys.time()-temp_time_start_sub, 3)




#test_2

{

temp_time_start_sub = Sys.time()

temp_extracted_trips =
  temp_file_list_use %>%
  furrr::future_map(
    ~{
      p()

      temp_extract =  arrow::read_parquet(.x)

      temp_pro = temp_extract %>%
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
        unnest(cols = solution_segments)  %>%
        .[,c('trip_id', 'device_id', 'provider_id'
             # ,'trip_raw_distance_m_ttl', 'start_utc_ts_ttl', 'end_utc_ts_ttl'
             ,'traj_idx', 'segment_id', 'segment_idx'
             ,"speed_kph"
             # ,"snap_count", "on_road_snap_count", "service_code"
        )] %>%
        mutate(
          speed_kph = round(speed_kph, 0)
          ,flag_border_link = case_when(
            segment_id %in% index_texas_omsids ~ 1
            ,T~0
          )) %>%
        group_by(trip_id) %>%
        mutate(flag_ttl = case_when(
          sum(flag_border_link)>0~T
          ,T~F
        )) %>%
        ungroup()

      # arrow::write_parquet(
      #   temp_pro
      #   ,here::here(
      #     "data/benchmark_save_out"
      #     ,str_glue("{gauntlet::strg_clean_datetime()}.parquet")
      #   )
      # )

      return(temp_pro)

    }) %>%
  rbindlist()

arrow::write_dataset(
  temp_extracted_trips
  ,here::here(
    "data/benchmark_bulk"
    ,str_glue("{gauntlet::strg_clean_datetime()}.parquet")
  )
)


temp_time_duration_sub = round(Sys.time()-temp_time_start_sub, 3)
}

