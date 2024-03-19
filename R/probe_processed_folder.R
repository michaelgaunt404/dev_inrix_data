#' Probe Processed Data Folder
#'
#' This function allows the user to probe a processed data folder. The folder path must be specified and should be of full length. The input parameter is called \code{folder_location}.
#'
#' @param folder_location The path to the processed data folder.
#' @return A list containing performance metrics for each of the *process_diagnostic.* output file types
#' @details The function outputs three main components for each file type:
#'   \itemize{
#'     \item \code{processed_files}: A vector of files that were processed. This can be compared against the files in the original raw folder to ensure all files were processed.
#'     \item \code{file_info}: Information about the first and last file written to memory, along with an overview of diagnostic information including the time taken for processing and the number of files processed within that time.
#'     \item \code{processing_metrics}: Basic metrics calculated for a sample of 500 files, including average, mean, and 95th percentile for various metrics such as rows, trips, and time taken for different processing stages.
#'   }
#'
#' @examples
#' folder_performance <- probe_processed_folder("//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk2")
#'
#' @export
#'
#'
probe_processed_folder = function(folder_location){
  # folder_location = "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk2"

    folder_perfromance_metics = c(
    "dgnstc_"
    # "summary_data_trips"
    # "processed_trips"
    ) %>%
    map(~{
      x = .x

      dt_files = data.table(
        files = list.files(
          here::here(folder_location)
        ))

      dt_files_process_diagnostic = dt_files %>%
        .[str_detect(files, x),] %>%
        .[order(files)]

      dt_files_process_diagnostic_fl = bind_rows(
        head(dt_files_process_diagnostic, 1)
        ,tail(dt_files_process_diagnostic, 1)
      )

      #output_#1
      df_file_info =
        bind_cols(
          dt_files_process_diagnostic_fl
          ,file.info(
            here::here(folder_location
                       ,dt_files_process_diagnostic_fl$files)) %>%
            rownames_to_column(var = "file") %>%
            select(!file)) %>%
        mutate(ttl_files = nrow(dt_files_process_diagnostic)
               ,duration = as.numeric(last(mtime) - first(mtime), units = "hours") %>% gauntlet::dgt2()
               ,files_per_hour = (ttl_files/duration) %>% gauntlet::dgt0())

      dt_files_process_diagnostic_sample = dt_files_process_diagnostic %>%
        sample_n(
          ifelse(nrow(dt_files_process_diagnostic)<=500
                 ,nrow(dt_files_process_diagnostic)
                 ,500))

      test =  here::here(folder_location
                         ,dt_files_process_diagnostic_sample$files)

      #output_#2
      bolo = arrow::open_dataset(
        test
      ) %>%
        summarise(
          rows_xmean = mean(rows)
          ,trips_xmean = mean(trips)
          ,time_read_xmean = mean(time_read)
          ,time_process_xmean = mean(time_process)
          ,time_write_xmean = mean(time_write)
          ,time_process_small_xmean = mean(time_process_small)
          ,time_write_small_xmean = mean(time_write_small)
          ,rows_xmedian = median(rows)
          ,trips_xmedian = median(trips)
          ,time_read_xmedian = median(time_read)
          ,time_process_xmedian = median(time_process)
          ,time_write_xmedian = median(time_write)
          ,time_process_small_xmedian = median(time_process_small)
          ,time_write_small_xmedian = median(time_write_small)
          ,rows_xq95 = quantile(rows, .95)
          ,trips_xq95 = quantile(trips, .95)
          ,time_read_xq95 = quantile(time_read, .95)
          ,time_process_xq95 = quantile(time_process, .95)
          ,time_write_xq95 = quantile(time_write, .95)
          ,time_process_small_xq95 = quantile(time_process_small, .95)
          ,time_write_small_xq95 = quantile(time_write_small, .95)
        ) %>%
        collect() %>%
        mutate(index = 1) %>%
        pivot_longer(cols = !c(index)) %>%
        separate(col = name, sep = "_x", into = c("col", "metric")) %>%
        pivot_wider(values_from = value, names_from = col)

      return(
        list(
          process_diagnostic_files = dt_files_process_diagnostic
          ,file_info = df_file_info
          ,processing_metics = bolo)
      )
    })


  return(folder_perfromance_metics[[1]])
}
