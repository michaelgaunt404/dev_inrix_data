#' Automates the process of deploying the probe_processed_folder function to multiple folders
#'
#' This function automates the process of deploying the probe_processed_folder function to multiple folders. It returns a data frame describing the current state of each processed folder, including the folder name, total number of files processed, processing duration, processing rate, and number of outstanding files remaining.
#'
#' @param folder_root The root directory containing the folders to process.
#' @param sample_size The sample size parameter to be passed to the probe_processed_folder function.
#' @return A data frame containing information about the processed folders.
#' @details This function utilizes the probe_processed_folder function to retrieve data for each folder found within the specified root directory. It then aggregates the results into a single data frame.
#' @examples
#' probe_processed_folder_auto("/path/to/root", sample_size = 100)
#'
#' @importFrom dplyr mutate select bind_cols unique
#' @importFrom purrr map_df
#' @importFrom stringr str_detect str_remove
#' @import qs qread
#' @importFrom here here
#' @importFrom glue str_glue
#' @importFrom stats nrow
probe_processed_folder_auto = function(folder_root, sample_size){
  #NOTE: remove folder removals at some point

  temp_dirs = list.files(folder_root) %>%
    .[!stringr::str_detect(., "mg")] %>%
    .[!stringr::str_detect(., "trips_usa_tx_202202_wk2")] %>% #bad_week
    here::here(folder_root, .)

  temp_dirs %>%
    map_df(~{
      x = .x

      message(str_glue("Getting data for:\n{x}"))
      temp = probe_processed_folder(x, sample_size = sample_size)

      file_load_vector = qs::qread(here::here(x,"file_load_vector.qs")) %>%
        mutate(file = gsub(".*trajs/", "\\1", file))

      file_load_vector_nrow = nrow(file_load_vector)

      processed = temp$file_info %>%
        mutate(files = gsub("_part.*", "\\1", files) %>%
                 str_remove("dgnstc_")) %>%
        select(files, ttl_files, duration, files_per_hour) %>%
        unique() %>%
        rename(files_prcssd = ttl_files) %>%
        mutate(files_ttl = file_load_vector_nrow
               ,pct_prcssed = round(files_prcssd/file_load_vector_nrow, 2)) %>%
        bind_cols(temp$processing_metics)

      return(processed)
    })

}
