#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# This is script installs commonly used packages.
#
# By: mike gaunt, michael.gaunt@wsp.com
#
# README: [[insert brief readme here]]
#-------- [[insert brief readme here]]
#
# *please use 80 character margins
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#library set-up=================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
library(tidyverse)
library(furrr)
# library(gauntlet)
library(arrow)
library(data.table)
library(here)
library(progressr)
library(polars)

#path set-up====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#content in this section should be removed if in production - ok for dev

#source helpers/utilities=======================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#content in this section should be removed if in production - ok for dev

#source data====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#content in this section should be removed if in production - ok for dev
#area to upload data with and to perform initial munging
#please add test data here so that others may use/unit test these scripts


#basic how-to example_1=========================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#you only need tol look at this if you are unfamilir with polars
write.csv(airquality, "airquality.csv", row.names = FALSE)
pl$read_csv("airquality.csv")

#lazy eval
scanned = pl$scan_csv("airquality.csv")
item = scanned$describe_plan()

scanned_planned = scanned$head(
  3)$filter(
    pl$col("Wind") > 10
  )

scanned_planned$describe_optimized_plan()
scanned_planned$describe_plan()
scanned_planned$collect_in_background()
#get data
scanned_planned$collect()


#basic how-to example_1=========================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


data_location = "C:/Users/gauntm/Documents/temp/week/trips_usa_tx_202208_wk4/date=2023-11-13/reportId=166942/v1/data/trajs"

data_location = "data/arrow_table_test"

p_files = list.files(
  here::here(data_location)
  ,"parquet"
) %>%
  here::here(data_location, .)

p_file = p_files[2]

p_df = arrow::read_parquet(p_file)
p_df_nrow = p_df[1,]
p_df_nrow_ec = p_df[1,] %>% dplyr::select(!c(start_utc_ts, end_utc_ts))
p_df_nrow_traj = p_df[1,c(1,9)]

arrow::write_parquet(p_df_nrow_ec, "p_df_nrow_ec.parquet")
string = "C:/Users/gauntm/Documents/010_projects/dev_inrix_data/data/arrow_table_test/part-01996-e98646e6-f504-4ea7-b564-53ab8be8d8ea-c000.gz.parquet"
scanned = pl$read_parquet(p_file)




data_location = "C:/Users/gauntm/Documents/temp/trips"
p_files = list.files(
  here::here(data_location)
  ,"parquet"
) %>%
  here::here(data_location, .)

p_file = p_files[1]
scanned = pl$scan_parquet("C:/Users/gauntm/Documents/temp/trips/*.parquet")
scanned = pl$scan_parquet(
  "C:/Users/gauntm/Documents/temp/week/date=2022-10-08/reportId=113730/v1/data/*.parquet"
                          ,n_rows = 100)


query = "select * from data limit 2"
pl$SQLContext(data = mtcars)$execute(query)$collect()





list.files("C:/Users/gauntm/Documents/temp/week/date=2022-10-08/reportId=113730/v1/data/trajs")











temp = arrow::read_parquet("df_fake_5e6.parquet")

model_object = temp %>%
  group_by(model) %>%
  nest() %>%
  mutate(mod_lm = map(data, ~lm(y~x, data = .x) %>% summary())) %>%
  mutate(mod_aic = map(mod_lm, ~.x$r.squared)) %>%
           select(!c(mod_lm))


model_object %>% arrow::write_parquet(
  "df_fake_nested.parquet"
)


scanned = pl$scan_parquet(
  "df_fake_nested.parquet")

scanned$head(1)$explode(c("data"))$unnest()$collect()
scanned$head(2)$unnest()$collect()
)$unlist()$unnest()$collect()


res = pl$SQLContext(frame = scanned)$execute(
  "select model, unnest(data) from frame;"
)$collect()


pl_tr
aq2$filter(
  pl$col("Month") <= 6
)$group_by(
  "Month"
)$agg(
  pl$col(c("Ozone", "Temp"))$count()
)$collect()

scanned$with_columns(
  pl$col(data)
)



data_location = "E:/010_projects/trips_usa_tx_202203_wk2/trajs"

p_files = list.files(
  here::here(data_location)
  ,"parquet"
) %>%
  here::here(data_location, .)

scanned = pl$scan_parquet(p_file)

yolo = scanned$select(
  pl$col(c("trip_id", "trajectories"))
)$explode(
  c("trajectories")
  )$unnest()$explode(
    c("solution_segments")
  )$unnest()$filter(
    pl$col("segment_id")$is_in(index_texas_omsids)
    )$collect()




data_location = "E:/010_projects/trips_usa_tx_202203_wk2/trajs"

p_files = list.files(
  here::here(data_location)
  ,"parquet"
) %>%
  here::here(data_location, .)

scanned = pl$scan_parquet(p_file)

yolo = scanned$select(
  pl$col(c("trip_id", "trajectories"))
)$explode(
  c("trajectories")
)$unnest()$explode(
  c("solution_segments")
)$unnest()$filter(
  pl$col("segment_id")$is_in(index_texas_omsids)
)$collect()














