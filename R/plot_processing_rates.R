
plot_processing_rates = function(folder_root){

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
          .[order(files)] %>%
          data.table()

        dt_files_temp = dt_files %>%
          .[,`:=`(
            process_time = files %>%
              str_remove('\\.parquet') %>%
              gsub(".*c000_", "\\1", .) %>%
              str_trunc(15, ellipsis = "") %>%
              parse_date_time(orders = c("Ymd HMS")))] %>%
          .[,`:=`(
            process_time_p = round_date(process_time, "5min")
            ,count = 1
            ,folder = x)] %>%
          .[,.(ttl_processed = sum(count))
            ,by = .(folder, process_time_p)] %>%
          .[,`:=`(processing_rate_p = ttl_processed*12)] %>%
          mutate(index_p = row_number()
                 ,ttl_processed_cumm = cumsum(ttl_processed)) %>%
          mutate(month = str_remove_all(folder, "trips_usa_tx_") %>%
                    gsub("_wk.*", "\\1",  .)
          )
      })

    })


  dt_files_temp_sd = df_processing_rate %>%
    pivot_longer(cols = c(processing_rate_p, ttl_processed_cumm)) %>%
    mutate(label = str_glue("{folder}\nValue: {value}")) %>%
    crosstalk::SharedData$new()

  temp_plot = bscols(
    widths = c(3, 3, 12)
    ,filter_select("id_1", "Select Variable:", dt_files_temp_sd, ~name)
    ,filter_select("id_2", "Select Month:", dt_files_temp_sd, ~month)
    ,plotly::plot_ly(
      dt_files_temp_sd, x = ~index_p, y = ~value, color = ~folder
      ,text = ~label, hoverinfo = "text"
      ,type = 'scatter', mode = 'lines') %>%
      plotly::layout(
        xaxis = list(title = "Processing Period (5min)")
        ,yaxis = list(title = "Variable Value")
      )
  )

  return(temp_plot)

}
























