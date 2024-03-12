
library(tidyverse)
library(furrr)
library(gauntlet)
library(arrow)
library(data.table)
library(here)
library(progressr)

library(polars)

df = pl$DataFrame(
  A = 1:5,
  fruits = c("banana", "banana", "apple", "apple", "banana"),
  B = 5:1,
  cars = c("beetle", "audi", "beetle", "beetle", "beetle")
)

# embarrassingly parallel execution & very expressive query language
df$sort("fruits")$select(
  "fruits",
  "cars",
  pl$lit("fruits")$alias("literal_string_fruits"),
  pl$col("B")$filter(pl$col("cars") == "beetle")$sum(),
  pl$col("A")$filter(pl$col("B") > 2)$sum()$over("cars")$alias("sum_A_by_cars"),
  pl$col("A")$sum()$over("fruits")$alias("sum_A_by_fruits"),
  pl$col("A")$reverse()$over("fruits")$alias("rev_A_by_fruits"),
  pl$col("A")$sort_by("B")$over("fruits")$alias("sort_A_by_B_by_fruits")
)




write.csv(airquality, "airquality.csv", row.names = FALSE)
pl$read_csv("airquality.csv")
scanned = pl$scan_csv("airquality.csv")

scanned$head(
  3)$filter(
    pl$col("Wind") > 10
  )$collect()



aq2 = pl$scan_parquet("./test/*.parquet")

as_polars_df(airquality)$write_parquet("airquality_1.parquet")

# eager version (okay)
aq = pl$scan_parquet("./test/airquality_1.parquet")

# lazy version (better)
aq = pl$scan_parquet("airquality.parquet")
aq$filter(
  # pl$col("Month") >= 6
  pl$col("Month")$is_in(c(6, 8))
)$group_by(
  "Month"
)$agg(
  pl$col(c("Ozone", "Temp"))$count()
)$collect()



length_of_dataframe <- 5e6

data <- data.frame(
  x = seq(1, length_of_dataframe)
  ,y = 2 * seq(1, length_of_dataframe) + rnorm(length_of_dataframe, mean = 0, sd = length_of_dataframe*.2)
  ,z = sample(letters, length_of_dataframe, replace = T)
) %>%
  mutate(model = rep(c("model_1", "model_2", "model_3", "model_4"), length_of_dataframe/4))


arrow::write_parquet(data, "df_fake_5e6.parquet")
aq = pl$scan_parquet("df_fake_5e6.parquet")
aq$group_by(
  "z"
)$agg(
  pl$col(c("y"))$count()
)$collect()






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














