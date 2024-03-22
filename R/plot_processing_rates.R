
pro %>%
  filter(is.na(CROSS_NM))



525602886_0


border_xing_table %>%
  filter(str_detect(seg_id, "525602886_0"))

border_xing_table %>% filter(str_detect(CROSS_NM, "Progreso International"))



# border_xing_table = qs::qread(here::here("//geoatfilpro1/cadd3/inrix_data/gis/"
,"table_extracted_border_crossing_locations_20240318.qs"))









# plot_processing_rates(
#   folder_root = "//geoatfilpro1/cadd3/inrix_data/processed_data"
# )

plot_processing_rates = function(folder_root){

# folder_root = "//geoatfilpro1/cadd3/inrix_data/processed_data"

  df_processing_rate = folder_root %>%
    list.files() %>%
    .[!str_detect(., "trips_usa_tx_202202_wk2")] %>%
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

        dt_files_temp = dt_files %>%
          mutate(process_time = files %>%
                   str_remove('\\.parquet') %>%
                   gsub(".*c000_", "\\1", .) %>%
                   str_trunc(15, ellipsis = "") %>%
                   parse_date_time(orders = c("Ymd HMS"))) %>%
          arrange(process_time) %>%
          mutate(index = row_number()) %>%
          mutate(running_time_secs = as.numeric(process_time-process_time[1], units = "secs")
                 ,running_time_hrs = running_time_secs/3600
                 ,processing_rate = index/running_time_hrs
                 ,folder = x)
      })

    })


 ( df_processing_rate %>%
    group_by(folder) %>%
    sample_n(5000) %>%
    ungroup() %>%
    ggplot(aes(index, processing_rate, color = folder)) +
    geom_line() +
    labs(y = "Processing Rate (files/hr)"
         ,x = "Number of Records Processed") ) %>%
    plotly::ggplotly()

}
























