
library(tidyverse)
library(furrr)
library(gauntlet)
library(arrow)
library(data.table)
library(here)
library(progressr)

library(polars)

index_texas_omsids = c("225591372_0", "435876345_0", "42800598_0", "20773334_0", "42801350_0"
                       , "140641539_0", "492211407_0", "20777051_1", "20765020_0", "525602886_0", "35185641_0"
                       , "145954711_1", "145954694_0"
                       , "365602439_0", "42806640_1", "357815727_0", "461204306_0", "102642059_0")

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




# data_location = "E:/010_projects/trips_usa_tx_202203_wk2/trajs"
# data_location = "E:/010_projects/bench_mark_folder"
data_location = "E:/010_projects/bench_mark_folder_sm"


p_files = list.files(
  here::here(data_location)
  ,"parquet"
) %>%
  here::here(data_location, .)

scanned = pl$scan_parquet(
  paste0(data_location, "/*.parquet")
  ,cache = F
  ,low_memory = T
)

#test explode method
tictoc::tic()
yolo = scanned$select(
  pl$col(c("trip_id", "device_id", "provider_id", "trajectories"))
)$explode(
  c("trajectories")
)$unnest()$explode(
  c("solution_segments")
)$unnest()$filter(
  pl$col("segment_id")$is_in(index_texas_omsids)
)$select(
  pl$col(c("trip_id", "device_id", "provider_id", "segment_id", "segment_idx"))
)$unique()

tictoc::toc()

tictoc::tic()
collected = yolo$collect()
tictoc::toc()

collected %>%
  as.data.table()
#   arrow::write_parquet("e_save.parquet")

#method to check dup device
tictoc::tic()
yolo = scanned$select(
  pl$col(c("trip_id", "device_id", "provider_id"))
)$unique()

tictoc::toc()


tictoc::tic()
collected = yolo$collect() %>%
  as.data.table()
tictoc::toc()

tictoc::tic()
collected %>%
  .[, .SD[.N >1], by = .(device_id)]
tictoc::toc()

tictoc::tic()
collected %>%
  group_by(device_id) %>%
  filter(n() > 1) %>%
  ungroup()
tictoc::toc()

collected %>%
  select(device_id, provider_id) %>%
  unique() %>%
  group_by(device_id) %>%
  filter(n() > 1) %>%
  ungroup()




test = p_files %>%
  furrr::future_map(
    ~{

        temp_data =
          arrow::read_parquet(.x) %>%
          .[, c('trip_id', 'device_id', 'provider_id')] %>%
          unique() %>%
          mutate(file = .x)


      return(temp_data)

    }) %>%
  rbindlist()



test_arrow = as_arrow_table(test)

collected = test_arrow %>%
  group_by(device_id) %>%
  filter(n() > 1) %>%
  ungroup() %>%
  arrange(device_id) %>%
  collect()

collected %>%
  select(file, device_id) %>%
  count(device_id) %>%
  mutate(count = 1) %>%
  count_percent_zscore(
    grp_c = c("n")
    ,grp_p = c()
    ,col = count
  )





#additional polars working file=================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
index_texas_omsids = c(
  "225591372_0", "435876345_0", "42800598_0", "20773334_0", "42801350_0"
  ,"140641539_0", "492211407_0", "20777051_1", "20765020_0", "525602886_0", "35185641_0"
  ,"145954711_1", "145954694_0", "365602439_0", "42806640_1", "357815727_0", "461204306_0", "102642059_0")

data_location_benchmark = "E:/010_projects/bench_mark_folder"
data_test_location = "C:FUsers/gauntm/Documents/010_projects/dev_inrix_data/data/arrow_table_test"
data_location_read = "E:/010_projects/trips_usa_tx_202203_wk2/trajs"
data_location_processed = "E:/010_projects/process_write"
# data_location_write = "E:/010_projects/process_write"

# ds_raw = arrow::open_dataset(data_location_read)
# ds_raw = arrow::open_dataset(data_location_read)

ds_processed = arrow::open_dataset(data_location_processed)
index_pro_trip_ids = ds_processed %>% collect() %>% pull(trip_id)

tictoc::tic()
ds_processed_col = ds_raw %>%
  .[1:10000,] %>%
  filter(trip_id %in% unique(ds_processed_col$trip_id))
tictoc::toc()


scanned = pl$scan_parquet(
  # paste0(data_location_benchmark, "/*.parquet")
  paste0(data_location, "/*.parquet")

  # ,n_rows = 600000
  ,cache = F
  ,low_memory = T
)

scanned_lazy = scanned$select(
  pl$col(c("trip_id", "device_id", "provider_id"
           , "trajectories"
  )))$
  filter(
    pl$col("trip_id")$is_in(index_pro_trip_ids)
  )
# explode(
#   c("trajectories")
# )$
# unnest()$
# explode(
#   c("solution_segments"))$
# unnest()$
# filter(
#   pl$col("segment_id")$is_in(index_texas_omsids)
# )$
# select(
#   pl$col(c("trip_id", "device_id", "provider_id", "segment_id", "segment_idx"))
# )$
# unique()

# $unnest()$explode(
#   c("solution_segments")

# )$select(
#   pl$col(c("trip_id", "device_id", "provider_id", "segment_id", "segment_idx"))
# unique(c)

tictoc::tic()
collected = scanned_lazy$collect()
tictoc::toc()



object.size()



collected %>% qs::qsave("tett.qs")












