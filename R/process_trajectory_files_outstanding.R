
process_trajectory_files_outstanding = function(
    data_location
    ,data_location_write_root_folder = "//10.120.118.10/cadd3/inrix_data/processed_data/"
    ,osm_match_links
    ,folder_suffix = NA
    ,number_of_batches = 1
    ,batch_size = 10000
    ,cores = 60
){

  {

    # data_location = "//geoatfilpro1/cadd3/inrix_data/trips_usa_tx_202208_wk2/date=2023-11-13/reportId=166940/v1/data/trajs"

  }

  #basic set up
  {

    data_week_pro = data_location %>%
      gsub(".*(trips_usa_tx_)", "\\1", .) %>%
      gsub("\\/date.*", "\\1", .)

    #create: folders and paths for output items
    data_location_write_processed = here::here(data_location_write_root_folder,  data_week_pro)
    stopifnot("Root folder does not exist" = dir.exists(data_location_write_processed))


    file_use_old = list.files(data_location, pattern = "par")
    file_use_pro = list.files(data_location_write_processed, pattern = "smmry_trips") %>%
      gsub(".*(part)", "\\1", .) %>%
      gsub("(c000).*", "\\1", .)  %>%
      paste0(., ".gz.parquet")

    file_use_new = file_use_old %>%
      .[!(file_use_old %in% file_use_pro)]

    message(str_glue("{length(file_use_new)} files will be processed"))
    check_continue = gauntlet::robust_prompt_used("want to proceed with processing")
    stopifnot("Stopping now..." = check_continue)

    #create: dt of files to process
    file_list_use_dt = data.table(
      row_index = 1:length(file_use_new)
      ,file = file_use_new
    )   %>%
      mutate(process_group = floor_divide(row_index/(round(length(file_use_new)/number_of_batches, 0)+1), 1)+1) %>%
      mutate(process_group_1 = floor_divide(row_index, batch_size))

    qs::qsave(
      file_list_use_dt
      ,here::here(
        data_location_write_processed
        ,str_glue("file_load_vector_outstanding_{gauntlet::strg_clean_datetime()}.qs")
      )
    )
    plan(multisession, workers = cores)
  }

  #actual processing
  {

    time_start = Sys.time()
    message(str_glue("Processing started at {time_start}"))

    #process
    #---- processing set in for loop
    #---- for loop is vestigual but can be deployed if need be
    #---- keeping it in as it doesn't hurt: mg_20240322
    for (i in 1:number_of_batches){

      message(str_glue("Processing batch {i} of {number_of_batches}"))

      file_list_use_batch = file_list_use_dt %>%
        filter(process_group == i) %>%
        pull(file)

      message(str_glue("Containing {length(file_list_use_batch)} files...."))


      file_list_use_batch %>%
        furrr::future_map(
          ~{

            x = .x

            file = gsub(".*(part-)", "\\1", x)
            file_sm = gsub(".gz.parquet.*", "\\1", file)

            #save: emergency file
            #---- again, not really required but doesn't hurt
            arrow::write_parquet(
              data.frame()
              ,here::here(
                data_location_write_processed
                ,"emergency_test"
                ,str_glue("emergency_test_{file_sm}.parquet")
              )
            )

            time_start_sub_process = Sys.time()

            temp_extract =  arrow::read_parquet(
              here::here(data_location, x))

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

            rm(temp_pro)
            rm(temp_extract)
            rm(temp_pro_small)
            gc()

          })

    }

    arrow::write_parquet(
      data.frame(
        time_start
        ,end_time = Sys.time()
        ,duration = round(Sys.time()-time_start, 3))
      ,here::here(
        data_location_write_processed
        ,str_glue("run_end_object_{data_week_pro}_{gauntlet::strg_clean_datetime()}.parquet")))
  }

}
