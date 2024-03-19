#' Convenience function for inspecting processed diagnostic files
#'
#' This function takes in a probe object created using the `processed_files` function,
#' a folder location containing processed files, and optional parameters for subsetting
#' and extracting files for inspection.
#'
#' @param probe_object A probe object created using the `processed_files` function.
#' @param folder_location The folder location containing processed files.
#' @param index_inspect_vector Optional. A vector of numerical indices to subset the list
#'   of probed files. Default is NA, which means no subsetting will be performed.
#' @param collect Logical. Indicates whether to extract the files based on the provided indices.
#'   Default is TRUE.
#' @param last Optional. An integer indicating the number of indices from the maximum index
#'   to inspect. If provided, overrides the `index_inspect_vector` parameter.
#' @return A processed dataset containing the inspected files.
#' @details This function takes a list of diagnostic files given the probe object
#'   and subsets that list with the provided indices. It then opens and optionally
#'   collects the files for inspection.
#' @examples
#' # Create a probe object
#' probe <- processed_files(...)
#'
#' # Inspect processed files
#' inspect_probe_files(probe_object = probe, folder_location = "path/to/files",
#'                     index_inspect_vector = c(1, 3, 5))
#'
#' @export
inspect_probe_files = function(probe_object, folder_location, index_inspect_vector = NA, collect = T, last = NA){

  if (!is.na(last)){
    tmp = nrow(probe_object$process_diagnostic_files)
    index_inspect_vector = c((tmp-last):tmp)
  }

  temp_file_vec = probe_object$process_diagnostic_files$files %>%
    .[index_inspect_vector] %>%
    paste0(folder_location, "/", .)

  stopifnot("Too many files returned - check inputs" = length(temp_file_vec) < 200)

  dataset_processed_files = temp_file_vec %>%
    arrow::open_dataset()

  if (collect){
    dataset_processed_files = dataset_processed_files %>%
      collect() %>%
      data.table()
  }

  return(dataset_processed_files)

}
