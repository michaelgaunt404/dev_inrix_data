#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# This is script benchmarks time to process and save out traj data
#
# By: mike gaunt, michael.gaunt@wsp.com
#
# README: explodes and then saves out traj files
#-------- benchmarks two methods - both using future_map
#-------- one saves as each item is batched
#-------- other saves after processing batch
#
# *please use 80 character margins
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#library set-up=================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#content in this section should be removed if in production - ok for dev
library(tidyverse)
library(furrr)
library(gauntlet)
library(arrow)
library(data.table)
library(here)
library(progressr)
library(polars)



#object import==================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~



#==================
scanned = pl$read_parquet(
  here::here(
    "//10.120.118.10/cadd3/inrix_data/benchmark_process_times"
    ,"*_time_object.parquet")) %>%
  as.data.frame() %>%
  mutate(total_time = case_when(
    df_size >= 5000 & total_time < 10~total_time*60, T~total_time
  )) %>%
  mutate(time_ttl = time_read + time_process + time_write
         ,process_rate = df_size/total_time
         ,special_id = str_remove(special_id, "individual_write_") %>%
           gsub("_1.*", "\\1", .)) %>%
  mutate(batch_group = case_when(
    df_size == 100~time_id, T~NA_character_
  )) %>%
  fill(batch_group) %>%
  mutate(batch_group = parse_date_time(str_trunc(batch_group, 15, ellipsis = ""), "ymd_HMS") %>%
           as.character() %>%
           gsub(".* ", "\\1", .))

scanned_smmry = scanned %>%
  select(
    special_id, batch_group, time_id, df_size, limit, cores, batch_limit
    ,total_time, process_rate
  ) %>%
  unique()


scanned %>%
  select(!time_id) %>%
  group_by(
    df_size, limit, cores, batch_limit
  ) %>%
  summarise(across(starts_with("time_"), ~mean(as.numeric(.x), na.rm = T)), .groups = "drop")

scanned_smmry %>%
  ggplot() +
  geom_point(aes(df_size, process_rate, color = as.factor(batch_group))) +
  geom_smooth(aes(df_size, process_rate, color = as.factor(batch_group)), method = "lm")
facet_grid(cols = vars(special_id))

scanned_smmry %>%
  ggplot() +
  geom_point(aes(df_size, total_time, color = as.factor(batch_group))) +
  geom_smooth(aes(df_size, total_time, color = as.factor(batch_group)))

scanned_smmry %>%
  ggplot() +
  geom_smooth(aes(df_size, process_rate, color = special_id))

scanned %>%
  filter(df_size == 5000) %>%
  ggplot() +
  geom_density(aes(time_ttl, color = time_id ))

scanned %>%
  select( special_id, time_id, df_size, total_time ) %>% unique() %>%
  filter(df_size == 5000)


test = scanned %>%
  select(!total_time) %>%
  pivot_longer(
    cols = c(time_read, time_process, time_write, time_ttl)
  ) %>%
  mutate(name = fct_relevel(name, 'time_read', 'time_process', 'time_write', 'time_ttl')) %>%
  filter(name == "time_ttl") %>%
  ggplot(
    aes(as.factor(df_size), value, color = name )
  ) +
  geom_boxplot() +
  stat_summary(fun=mean, geom="point", shape=23, size=4)

plotly::ggplotly(test)








