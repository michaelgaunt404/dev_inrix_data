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
#processed texas link ids
index_texas_omsids = here(
  "data/gis"
  ,"texas_spatial_object_list_all_entries.qs") %>%
  qs::qread() %>%
  .[['index_texas_omsids']] %>%
  sort()

# data_location = "data/bench_mark_folder_sm" %>% here()
# data_location = "data/bench_mark_folder" %>% here()
# data_location = "//10.120.118.10/cadd3/inrix_data/bench_mark_folder"
data_location = "//10.120.118.10/cadd3/inrix_data/trips_usa_tx_202202_wk2/date=2023-11-16/reportId=167124/v1/data/trajs"
# data_location = "//10.120.118.10/cadd3/inrix_data/trips_usa_tx_202208_wk1/date=2023-11-13/reportId=166939/v1/data/trajs"
data_location_write = "//10.120.118.10/cadd3/inrix_data/benchmark_outputs/bm_small_network_to_network"

#processing options=============================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

limit = 1000 #NA indicates no reduction in files
# future::availableCores() #show cores
cores = 30 #choose cores
batch_limit = 250 #choose batch size - had been somewhat optimized but might need to be re optimized

files = list.files(data_location, pattern = "par") %>% here::here(data_location, .) #raw files
file_list_use = files[1:ifelse(is.na(limit), length(files), limit)] #reduced - if limit set

#process_method_1===============================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#method opens and processing files and immediately saves out

# index_limit = seq(100, 1000, 100)
# index_limit = c(100, 200)

index_limit = c(1000, 2000, 5000)

for (i in 1:length(index_limit)){

  limit = index_limit[i]

  print(limit)

  files = list.files(data_location, pattern = "par") %>% here::here(data_location, .) #raw files
  file_list_use = files[1:ifelse(is.na(limit), length(files), limit)]

  print(length(file_list_use))

  plan(multisession, workers = cores)

  special_id = "individual_write_network_to_network_1B1"
  time_id = gauntlet::strg_clean_datetime()

  {
    time_start = Sys.time()

    progressr::with_progress({
      p <- progressr::progressor(steps = length(file_list_use))

      temp_extracted_trips =
        file_list_use %>%
        furrr::future_map(
          ~{
            p()

            time_start_sub_process = Sys.time()

            temp_extract =  arrow::read_parquet(.x)

            time_read = Sys.time()

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
                   ,'start_utc_ts_ttl', 'end_utc_ts_ttl'
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

            time_process = Sys.time()

            arrow::write_parquet(
              temp_pro
              ,here::here(
                data_location_write
                ,str_glue("{gauntlet::strg_clean_datetime()}.parquet")
              )
            )

            time_write = Sys.time()

            arrow::write_parquet(
              data.frame(
                special_id
                ,time_id
                ,df_size = length(file_list_use), limit, cores, batch_limit
                ,time_read = as.numeric(time_read - time_start_sub_process)
                ,time_process = as.numeric(time_process - time_read)
                ,time_write = as.numeric(time_write - time_process)
              ), here::here(data_location_write, str_glue("{gauntlet::strg_clean_datetime()}_time_sub_process.parquet")))

          })

    })

    total_time = round(Sys.time()-time_start, 3)

    scanned = pl$scan_parquet(
      here::here(data_location_write
                 ,"*_time_sub_process.parquet")

    )$collect() %>%
      as.data.frame() %>%
      mutate(total_time = total_time %>% as.numeric())

    arrow::write_parquet(
      scanned
      ,here::here(
        "//10.120.118.10/cadd3/inrix_data/benchmark_process_times"
        ,str_glue("{special_id}_{time_id}_time_object.parquet")))

    list.files(data_location_write) %>%
      paste0(data_location_write, "/", .) %>%
      file.remove()

  }

}

#test_2-----

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









































