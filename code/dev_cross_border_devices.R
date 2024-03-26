library(tidyverse)
library(furrr)
library(arrow)
library(data.table)
library(here)
library(progressr)
library(polars)

data_location_root = "//geoatfilpro1/cadd3/inrix_data"



get_device_OD_for_summary_tables = function(processed_folder_location){

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

    df_devices = unique(df_summary$device_id) # df_summary_od %>% group_by(device_id) %>% summarise(count = n())

    # get the trips file location based on the process file folder name
    # search all the subdirectories and find match "/v1/data/trips"
    trips_data_location = here::here(data_location_root, basename(processed_folder_location))
    dirs <- list.dirs(trips_data_location)
    dirs <- grep("/v1/data/trips", dirs, value = TRUE)

    # merge all the trips into one dataframe with tripid and lat/long only
    trip_files <- list.files(dirs, full.names = TRUE, pattern = "parquet")

    dataframes <- list()

    i <- 1

    for (file in trip_files) {
      print(i)

      df <- arrow::read_parquet(file) %>% .[,c("trip_id","device_id","start_date","end_date","start_lat","start_lon" ,"end_lat","end_lon")] %>% filter( device_id %in% df_devices)
      dataframes <- c(dataframes, list(df))
      i <- i + 1
    }

    print("combine the trips dataframe")
    df_combinedtrips <- do.call(rbind, dataframes)

    if("start_lat" %in% names(df_summary)){
      df_combinedtrips <- left_join(df_combinedtrips,df_summary,by="trip_id") %>% .[,c("trip_id","device_id.x","start_date","end_date","start_lat.x","start_lon.x" ,"end_lat.x","end_lon.x","CROSS_NM","flag_seq")]

    }else{
      df_combinedtrips <- left_join(df_combinedtrips,df_summary,by="trip_id") %>% .[,c("trip_id","device_id.x","start_date","end_date","start_lat","start_lon" ,"end_lat","end_lon","CROSS_NM","flag_seq")]

    }


    df_combinedtrips <- df_combinedtrips[order(df_combinedtrips$device_id.x, df_combinedtrips$start_date), ]

    colnames(df_combinedtrips) <- c("trip_id","device_id.x","start_date","end_date","start_lat","start_lon","end_lat","end_lon" ,"segment_id","flag_direction","CROSS_NM","flag_seq")

    df_combinedtrips <- df_combinedtrips[, !(names(df_combinedtrips) %in% c("segment_id", "flag_direction"))] %>% unique()


    arrow::write_parquet(df_combinedtrips,here::here(processed_folder_location, "trips_device_od.parquet"))

  }


}




data_processed_location = here::here(data_location_root, "processed_data")
data_processed_weeks = list.files(data_processed_location) %>% here::here(data_processed_location, .)

for (data_processed_week in  data_processed_weeks) {
  print(data_processed_week)
  if(!grepl(paste0("trips_usa_tx_202202_wk2", "$"), data_processed_week) &&
     !grepl(paste0("trips_usa_tx_202202_wk4", "$"), data_processed_week)){
    get_device_OD_for_summary_tables(data_processed_week)
  }
}



