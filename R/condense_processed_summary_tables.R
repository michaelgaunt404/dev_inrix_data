#' Convenience function for condensing processed summary tables
#'
#' This function opens a series of Parquet files that contain summary data from processing nrx data.
#' The summary files typically hold trip IDs and device IDs and can be used in conjunction with
#' the trips folders in the nrx data. The function utilizes parallel processing to open all the
#' summary tables and then combines them into a single data table.
#'
#' @param folder_location The folder location containing the Parquet files.
#' @param cores The number of CPU cores to use for parallel processing.
#' @return A single data table containing the combined information from all the summary files.
#' @details This function uses parallel processing to open and combine multiple summary files
#'   efficiently. It handles any errors that may occur during file reading.
#' @examples
#' # Condense processed summary tables
#' combined_data <- condense_processed_summary_tables(folder_location = "path/to/summary/files",
#'                                                   cores = 4)
#'
#' @importFrom data.table rbindlist
#' @importFrom furrr future_map
#' @importFrom arrow read_parquet
#' @importFrom stringr str_detect
#' @importFrom here here
#' @export
condense_processed_summary_tables = function(folder_location, cores){

  plan(multisession, workers = cores)

  dt_files = data.table(
    files = list.files(
      here::here(folder_location)
    )) %>%
    .[str_detect(files, "summary"),] %>%
    .[order(files)]

  total_files = nrow(dt_files)
  message("Extraction started....")
  start = Sys.time()
  print(start)

  temp_extracted_trips =
    dt_files %>%
    pull(files) %>%
    # .[1:1] %>%
    furrr::future_map(
      ~{
        x = .x

        tryCatch({
          temp_file = arrow::read_parquet(
            here::here(folder_location, x))

        }, error = function(e) {
          # Error handling code

          message(str_glue("{gauntlet::strg_make_space_2()}WARNING ---- An error occurred:\n{x}\nBad file......{gauntlet::strg_make_space_2(last = F)
}"))


        })


      }) %>%
    rbindlist()

  end = Sys.time()

  print(end)
  message("Extraction ended....")

  return(temp_extracted_trips)

}

