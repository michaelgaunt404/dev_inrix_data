


library(tidyverse)
library(furrr)
library(gauntlet)
library(arrow)
library(data.table)
library(here)
library(progressr)




# data_location = "E:/010_projects/trips_usa_tx_202203_wk2/trajs"
# data_location = "E:/010_projects/trips_usa_tx_202208_wk4/date=2023-11-13/reportId=166942/v1/data/trajs"
data_location = here::here("data/arrow_table_test")

ds = open_dataset(
  data_location
  ,format = c("parquet")
)

tictoc::tic()

object = ds %>%
  dplyr::select(
    trip_id, device_id, provider_id
    ,trip_raw_distance_m, trip_raw_duration_millis
    ,trajectories, start_utc_ts, end_utc_ts) %>%
  rename(start_ttl = start_utc_ts, end_ttl = end_utc_ts) %>%
  collect()  %>%
  unnest(trajectories) %>%
  unnest(cols = c(solution_segments)) %>%
  mutate(error_codes_l = map_int(error_codes,~.x[1])
         ,start = first(start_utc_ts)
         ,end = last(end_utc_ts)) %>%
  group_by(trip_id, traj_idx, start, end, traj_raw_distance_m, traj_raw_duration_millis) %>%
  summarise(
    record_count = n()
    ,length_m_sum = sum(length_m)
    ,start_na = sum(is.na(start_utc_ts))
    ,end_na = sum(is.na(end_utc_ts))
    ,errr_100 = sum(error_codes_l == 100, na.rm = T)
    ,errr_101 = sum(error_codes_l == 101, na.rm = T)
    ,errr_102 = sum(error_codes_l == 102, na.rm = T)
    ,errr_103 = sum(error_codes_l == 103, na.rm = T)
    ,errr_104 = sum(error_codes_l == 104, na.rm = T)
    ) %>%
ungroup()

tictoc::toc()



plan(multisession, workers = 32)

tictoc::tic()
summry_object = list.files(
  data_location, pattern = "par"
) %>%
  here::here(data_location, .) %>%
  future_map(~{

    temp = arrow::read_parquet(.x, as_data_frame = T)

    temp %>%
      dplyr::select(
        trip_id, device_id, provider_id
        ,trip_raw_distance_m, trip_raw_duration_millis
        ,trajectories, start_utc_ts, end_utc_ts) %>%
      rename(start_ttl = start_utc_ts, end_ttl = end_utc_ts) %>%
      unnest(trajectories) %>%
      unnest(cols = c(solution_segments)) %>%
      mutate(error_codes_l = map_int(error_codes,~.x[1])
             ,start = first(start_utc_ts)
             ,end = last(end_utc_ts)) %>%
      group_by(
        trip_id, traj_idx
        ,start_utc_ts, start, end_ttl, end
        ,traj_raw_distance_m, traj_raw_duration_millis) %>%
      summarise(
        record_count = n()
        ,length_m_sum = sum(length_m)
        ,start_na = sum(is.na(start_utc_ts))
        ,end_na = sum(is.na(end_utc_ts))
        ,errr_100 = sum(error_codes_l == 100, na.rm = T)
        ,errr_101 = sum(error_codes_l == 101, na.rm = T)
        ,errr_102 = sum(error_codes_l == 102, na.rm = T)
        ,errr_103 = sum(error_codes_l == 103, na.rm = T)
        ,errr_104 = sum(error_codes_l == 104, na.rm = T)
      ) %>%
      ungroup()

  }) %>%
  rbindlist()
tictoc::toc()

