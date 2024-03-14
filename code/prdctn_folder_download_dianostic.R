#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# This is script is used to monitor the download status of inrix folders
#
# By: mike gaunt, michael.gaunt@wsp.com
#
# README: runs on loop
#-------- prints html widget that can be inspected
#
# *please use 80 character margins
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#library set-up=================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#content in this section should be removed if in production - ok for dev
library(magrittr)
library(tidyverse)
library(lubridate)

#path set up====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
root_folder = "//geoatfilpro1/cadd3/inrix_data" %>% here::here()

#object import==================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#this clears - do not do this
# df = data.frame()

for (i in 1:1000){

  dirs = list.dirs(root_folder, recursive = F) %>%
    .[str_detect(., "trips_usa")]

  time_stamp = Sys.time()

  df_temp = dirs %>%
    map_df(~{


      data.frame(

        folder = gsub(".*inrix_data/", "\\1", .x)
        ,file_count = .x %>%
          list.dirs(recursive = F) %>%
          list.dirs(recursive = F) %>%
          list.dirs(recursive = F) %>%
          list.dirs(recursive = F) %>%
          .[str_detect(., "data$")] %>%
          list.dirs(recursive = F) %>%
          .[str_detect(., "trajs$")] %>%
          list.files() %>%
          length()
        ,time_stamp = time_stamp
      )
    })

  # print(df_temp)

  df = bind_rows(
    df
    ,df_temp
  ) %>%
    unique() #%>%
  # filter(folder != "trips_usa_tx_202208_wk4" &
  #        folder != "trips_usa_tx_202208_wk3" &
  #         !str_detect(folder, "//1"))

  # Sys.sleep(30)



  download_diagnostic = df %>%
    arrange(folder, time_stamp) %>%
    group_by(folder) %>%
    mutate(
      index = row_number()
      ,diff = file_count-lag(file_count)) %>%
    ungroup()

  download_process = download_diagnostic %>%
    mutate(label = str_glue("{folder}\nTotal Count: {file_count}\nDiff: {diff}\nTimestamp: {lubridate::as_datetime(time_stamp) %>% lubridate::ymd_hms()}")) %>%
    plotly::plot_ly(
      x = ~index, y = ~file_count,  split = ~folder, mode = 'lines+markers'
      ,text = ~label, hoverinfo = "text"
    )

  htmlwidgets::saveWidget(
    download_process
    ,here::here("//geoatfilpro1/cadd3/inrix_data/folder_download_dianostic", "folder_download_dianostic.html")
  )

  Sys.sleep(60*10)


}



