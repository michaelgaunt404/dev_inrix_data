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

##method_1------
#processed texas link ids
# index_texas_omsids = here(
#   "data/gis"
#   ,"texas_spatial_object_list_all_entries.qs") %>%
#   qs::qread() %>%
#   .[['index_texas_omsids']] %>%
#   sort()

##method_2------
#note: this is an alternative method to make the OSM border crossing links
#----- this is beacuse old method had bad links
texas_border_locations_object = qs::qread(
  here::here(
    "//geoatfilpro1/cadd3/inrix_data/gis"
    ,"extracted_border_crossing_locations_20240318.qs"
  )
)

index_texas_omsids = texas_border_locations_object %>%
  .[["border_locations_comb_pro"]] %>%
  .[,"seg_id"]

index_texas_omsids_pro = c(
  index_texas_omsids
  ,paste0("-", index_texas_omsids)
)


#defining_Data_IO_locations=====================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# data_location = "data/bench_mark_folder_sm" %>% here()
# data_location = "data/bench_mark_folder" %>% here()
# data_location = "//10.120.118.10/cadd3/inrix_data/bench_mark_folder"
# data_location = "//10.120.118.10/cadd3/inrix_data/trips_usa_tx_202202_wk2/date=2023-11-16/reportId=167124/v1/data/trajs"
# data_location = "//10.120.118.10/cadd3/inrix_data/trips_usa_tx_202208_wk1/date=2023-11-13/reportId=166939/v1/data/trajs"
# data_location_write = "//10.120.118.10/cadd3/inrix_data/benchmark_outputs/bm_small_network_to_network"

#processing options=============================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# limit = 1000 #NA indicates no reduction in files
# # future::availableCores() #show cores
# cores = 30 #choose cores
# # batch_limit = 250 #choose batch size - had been somewhat optimized but might need to be re optimized
#
# files = list.files(data_location, pattern = "par") %>% here::here(data_location, .) #raw files
# file_list_use = files[1:ifelse(is.na(limit), length(files), limit)] #reduced - if limit set

#process_method_1===============================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#method opens and processing files and immediately saves out

#specify compute resources
limit = NA #NA indicates no reduction in files
cores = 60 #specify number of cars

#specify IO locations
data_location = "//10.120.118.10/cadd3/inrix_data/trips_usa_tx_202202_wk2/date=2023-11-16/reportId=167124/v1/data/trajs"
data_location_write_root_folder = "//10.120.118.10/cadd3/inrix_data/processed_data/" #this should be root location - subfolders will be made

#specify object containing border crossing locations
osm_match_links = index_texas_omsids_pro



{
  #everything within this bracket should be automatically completed
  #---- does not need user input
  #errors/warning may occur if locations are non-exisitent/bad
data_location_write_root_folder_pro = here::here(data_location_write_root_folder)

stopifnot("Root folder does not exist" = dir.exists(data_location_write_root_folder_pro))

data_week_pro = data_location %>%
  gsub(".*(trips_usa_tx_)", "\\1", .) %>%
  gsub("\\/date.*", "\\1", .)

data_location_write_processed = paste0(data_location_write_root_folder_pro, "/", data_week_pro)

dir.create(data_location_write_processed)

stopifnot("Write folder does not exist" = dir.exists(data_location_write_processed))

message(str_glue("{gauntlet::strg_make_space_2()}Processed data will be saved at this location:\n{data_location_write_processed}\n{gauntlet::strg_make_space_2()}"))

files = list.files(data_location, pattern = "par") %>% here::here(data_location, .) #raw files
file_list_use = files[1:ifelse(is.na(limit), length(files), limit)]

data.table(
  row_index = 1:length(file_list_use)
    ,file = file_list_use
) %>% qs::qsave(
  here::here(
    data_location_write_processed
    ,"file_load_vector.qs"
  )
)

print(length(file_list_use))

plan(multisession, workers = cores)
}
# special_id = "individual_write_network_to_network_1B1"
# time_id = gauntlet::strg_clean_datetime()

{
  time_start = Sys.time()

  # progressr::with_progress({
    # p <- progressr::progressor(steps = length(file_list_use))

    temp_extracted_trips =
      gsungs[1] %>%
      furrr::future_map(
        ~{
          # p()

          x = .x

          file = gsub(".*(part-)", "\\1", x)
          file_sm = gsub(".gz.parquet.*", "\\1", file)

          time_start_sub_process = Sys.time()

          temp_extract =  arrow::read_parquet(x)

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
                segment_id %in% osm_match_links ~ 1
                ,T~0)
              ,file = file
              ,folder = data_week_pro) %>%
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
              data_location_write_processed
              ,str_glue("prcssd_{data_week_pro}_{file_sm}_{gauntlet::strg_clean_datetime()}.parquet")
            )
          )

          time_write = Sys.time()

          temp_pro_small = temp_pro %>%
            filter(flag_border_link == 1) %>%
            select(trip_id, device_id, file, folder, traj_idx, segment_id, segment_idx) %>%
            unique()

          time_process_small = Sys.time()

          arrow::write_parquet(
            temp_pro_small
            ,here::here(
              data_location_write_processed
              ,str_glue("smmry_{data_week_pro}_{file_sm}_{gauntlet::strg_clean_datetime()}.parquet")
            )
          )

          time_write_small = Sys.time()

          #module to save run diagnostics
          #only saves every tenth item
          if(runif(1, 0, 1) > 0){
            arrow::write_parquet(
              data.frame(
                cores
                ,data_week_pro
                ,file
                ,rows = nrow(temp_pro)
                ,trips = nrow(temp_pro_small)
                ,time_read = as.numeric(time_read - time_start_sub_process)
                ,time_process = as.numeric(time_process - time_read)
                ,time_write = as.numeric(time_write - time_process)
                ,time_process_small = as.numeric(time_process_small - time_write)
                ,time_write_small = as.numeric(time_write_small - time_process_small))
              ,here::here(data_location_write_processed
                          ,str_glue("dgnstc_{data_week_pro}_{file_sm}_{gauntlet::strg_clean_datetime()}.parquet")
              ))

          }

        })

  # })

  arrow::write_parquet(
    data.frame(
      time_start
      ,end_time = Sys.time()
      ,duration = round(Sys.time()-time_start, 3))
    ,here::here(
      data_location_write_processed
      ,str_glue("run_end_object_{data_week_pro}_{gauntlet::strg_clean_datetime()}.parquet")))


  # list.files(data_location_write_processed) %>%
  #   paste0(data_location_write_processed, "/", .) %>%
  #   file.remove()

}

yolo = arrow::read_parquet(
  here::here(
    data_location_write_processed
    ,"run_end_object_trips_usa_tx_202202_wk2_20240318_190618988477.parquet"))


yolo


