process_trajectory_files = function(
    data_location
    ,data_location_write_root_folder = "//10.120.118.10/cadd3/inrix_data/processed_data/"
    ,osm_match_links
    ,folder_suffix = NA
    ,number_of_batches = 1
    ,batch_size = 10000
    ,cores = 60
){

  #basic set up
  {
    #create: folders and paths for output items
    data_location_write_root_folder_pro = here::here(data_location_write_root_folder)
    stopifnot("Root folder does not exist" = dir.exists(data_location_write_root_folder_pro))

    data_week_pro = data_location %>%
      gsub(".*(trips_usa_tx_)", "\\1", .) %>%
      gsub("\\/date.*", "\\1", .)

    if (!is.na(folder_suffix)){data_week_pro = paste0(data_week_pro, folder_suffix)}

    data_location_write_processed = paste0(data_location_write_root_folder_pro, "/", data_week_pro)
    dir.create(data_location_write_processed)
    stopifnot("Write folder does not exist" = dir.exists(data_location_write_processed))
    message(str_glue("{gauntlet::strg_make_space_2()}Processed data will be saved at this location:\n{data_location_write_processed}\n{gauntlet::strg_make_space_2()}"))

    #

    #create: emergency test location
    #this directory holds "emergency test" files as they are written out
    #---- mentioned files are blank files whose names indicate which files have been processed
    #---- this was needed for processing debugging and not really required anymore
    #---- still keeping here for further use: mg_20240322
    here::here(
      data_location_write_processed
      ,"emergency_test") %>%
      dir.create()

    file_list_use = list.files(data_location, pattern = "par") %>% here::here(data_location, .) #raw files
    # file_list_use = files[1:ifelse(is.na(limit), length(files), limit)] #removing obsolete

    #check: continue processing
    message(str_glue("{length(file_list_use)} files will be processed"))
    check_continue = gauntlet::robust_prompt_used("want to proceed with processing")
    stopifnot("Stopping now..." = check_continue)

    #create: dt of files to process
    file_list_use_dt = data.table(
      row_index = 1:length(file_list_use)
      ,file = file_list_use
    )   %>%
      mutate(process_group = floor_divide(row_index/(round(length(file_list_use)/number_of_batches, 0)+1), 1)+1) %>%
      mutate(process_group_1 = floor_divide(row_index, batch_size))

    qs::qsave(
      file_list_use_dt
      ,here::here(
        data_location_write_processed
        ,"file_load_vector.qs"
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
