library(tidyverse)
library(furrr)
library(arrow)
library(data.table)
library(here)
library(progressr)
library(polars)


get_OD_for_summary_tables = function(processed_folder_location){
  # get the latest condensed summary file
  file_info <- file.info(list.files(processed_folder_location, full.names = TRUE, pattern = "condensed"))
  file_info$file_name <- rownames(file_info)
  file_info <- file_info[order(file_info$ctime, decreasing = TRUE),]

  if(!is.na(file_info[1,]$file_name)){

    print(file_info[1,]$file_name)
    # read summary file
    df_summary = arrow::read_parquet(file_info[1,]$file_name)

    duplicate_rows <- duplicated(df_summary)
    df_summary <- df_summary[!duplicate_rows, ]

    # get the trips file location based on the process file folder name
    # search all the subdirectories and find match "/v1/data/trips"
    trips_data_location = here::here(data_location_root, basename(processed_folder_location))
    dirs <- list.dirs(trips_data_location)
    dirs <- grep("/v1/data/trips", dirs, value = TRUE)

    # merge all the trips into one dataframe with tripid and lat/long only
    trip_files <- list.files(dirs, full.names = TRUE, pattern = "parquet")

    dataframes <- list()

    for (file in trip_files) {
      print(file)
      df <- arrow::read_parquet(file) %>% .[,c("trip_id", "start_lat", "start_lon", "end_lat", "end_lon")]
      dataframes <- c(dataframes, list(df))
    }

    print("combine the trips dataframe")
    df_combinedtips <- do.call(rbind, dataframes)

    print("merge summary dataframe with trips")
    df_merge <- left_join(df_summary,df_combinedtips,by="trip_id")

    print("write summary dataframe to disk")
    arrow::write_parquet(df_merge,
                         here::here(processed_folder_location, "condensed_summary_with_od.parquet"))


  }

}

get_OD_for_summary_tables(
  "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk4")





# [1] "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk2"
# [1] "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk4"
# [1] "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202204_wk1"
# [1] "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202204_wk2"
# [1] "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202204_wk3"
# [1] "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202204_wk4"
# [1] "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202208_wk1"
# [1] "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202208_wk2"


data_location_root = "//geoatfilpro1/cadd3/inrix_data"

data_processed_location = here::here(data_location_root, "processed_data")

data_processed_weeks = list.files(data_processed_location) %>% here::here(data_processed_location, .)

for (data_processed_week in  data_processed_weeks) {
  print(data_processed_week)
  #get_OD_for_summary_tables(data_processed_file)
}
