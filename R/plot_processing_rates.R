
plot_processing_rates = function(folder_root, sample_size = 5000){

# folder_root = "//geoatfilpro1/cadd3/inrix_data/processed_data"

  df_processing_rate = folder_root %>%
    list.files() %>%
    .[!str_detect(., "trips_usa_tx_202202_wk2")] %>%
    # .[7] %>%
    # .[str_detect(., "trips_usa_tx_202208_wk2")] %>%
    # .[1] %>%

    map_df(~{
      x = .x

      tryCatch({

        dt_files = data.table(
          files = list.files(
            here::here(
              folder_root, x)
          )) %>%
          .[str_detect(files, "dgnstc_|dia"),] %>%
          .[order(files)]

        dt_files %>%
          .[str_detect(files, "240322"),]

        dt_files_temp = dt_files %>%
          mutate(process_time = files %>%
                   str_remove('\\.parquet') %>%
                   gsub(".*c000_", "\\1", .) %>%
                   str_trunc(15, ellipsis = "") %>%
                   parse_date_time(orders = c("Ymd HMS"))) %>%
          arrange(process_time) %>%
          mutate(date = as_date(process_time)) %>%
          group_by(date) %>%
          mutate(index = row_number()) %>%
          mutate(running_time_secs = as.numeric(process_time-process_time[1], units = "secs")
                 ,running_time_hrs = running_time_secs/3600
                 ,processing_rate = index/running_time_hrs
                 ,folder = x)
      })

    })


 ( df_processing_rate %>%
     mutate(date = as_date(process_time)) %>%
    # group_by(folder, date) %>%
    # sample_n(sample_size, replace = T) %>%
    # ungroup() %>%
    ggplot(aes(index , processing_rate, color = folder, group = date)) +
    geom_line() +
    labs(y = "Processing Rate (files/hr)"
         ,x = "Number of Records Processed") ) %>%
    plotly::ggplotly()

}
























