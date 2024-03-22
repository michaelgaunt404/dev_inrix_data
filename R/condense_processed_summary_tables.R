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
condense_processed_summary_tables = function(folder_location, cores, border_xing_table){

  # border_xing_table = qs::qread(here::here("//geoatfilpro1/cadd3/inrix_data/gis/", "table_extracted_border_crossing_locations_20240318.qs"))
  # folder_location = "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk4"
  # cores = 60

  plan(multisession, workers = cores)

  dt_files = data.table(
    files = list.files(
      here::here(folder_location)
    )) %>%
    .[str_detect(files, "summary_trips|smmry_trips"),] %>%
    .[str_detect(files, "part"),]

  total_files = nrow(dt_files)
  start = Sys.time()

  message(str_glue("{gauntlet::strg_make_space_2()}Extraction started.... {start}\nTotal number of files: {total_files}"))

  temp_file =
    dt_files %>%
    pull(files) %>%
    furrr::future_map(
      ~{
        x = .x

        tryCatch({
          temp_file = arrow::read_parquet(
            here::here(folder_location, x))

        }, error = function(e) {
          message(str_glue("{gauntlet::strg_make_space_2()}WARNING ---- An error occurred:\n{x}\nBad file......{gauntlet::strg_make_space_2(last = F)
}"))
        })

      }) %>%
    rbindlist(fill = T)

  pro = temp_file %>%
 # test =  tt %>%
    merge(border_xing_table %>% select(!flag_dupe)
          ,by.x = "segment_id", by.y = "seg_id", all.x = T) %>%
    arrange(trip_id, device_id, traj_idx, segment_idx) %>%
    group_by(trip_id, device_id, traj_idx, flag_direction) %>%
    slice_head(n = 1) %>%
    ungroup() %>%
    group_by(trip_id, device_id, traj_idx) %>%
    mutate(flag_dir_subtrip_strt = first(flag_direction)
           ,flag_dir_subtrip_end = last(flag_direction)) %>%
    group_by(trip_id, device_id) %>%
    mutate(flag_dir_trip_strt = first(flag_direction)
           ,flag_dir_trip_end = last(flag_direction)
           ,tll_exit = sum(flag_direction %in% c("exting", "exiting"))
           ,tll_enter = sum(flag_direction == "entering")
           ,flag_seq = paste0(flag_direction, collapse = "-")) %>%
    ungroup() %>%
    mutate(flag_seq = case_when(
      flag_seq == "NA"~NA_character_, T~flag_seq
    )) %>%
    data.frame()

  # pro %>%
  #   mutate(count = 1) %>%
  #   gauntlet::count_percent_zscore(
  #     grp_c = c("CROSS_NM", "flag_seq")
  #     ,grp_p = c("CROSS_NM")
  #     ,col = count
  #   ) %>%
  #   arrange(desc(count)) %>%
  #   mutate(percent_cum = cumsum(percent)) %>%
  #   arrange(CROSS_NM, desc(count)) %>%
  #   group_by(CROSS_NM) %>%
  #   slice_head(n = 3) %>%
  #   ungroup()


  end = Sys.time()

  message(str_glue("Extraction ended at {end}....{gauntlet::strg_make_space_2(last = F)}"))

  return(pro)

}





