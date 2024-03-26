#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# This is script explores spatial object for facility locations.
#
# By: mike gaunt, michael.gaunt@wsp.com
#
# README: from sebastian
#-------- CSV of spatial object
#-------- for freight facilities
#
# *please use 80 character margins
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#library set-up=================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#content in this section should be removed if in production - ok for dev
library(data.table)
library(tidyverse)
library(tidyfast)
library(sf)


#path set-up====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# folder = "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202204_wk1"
folder = "//geoatfilpro1/cadd3/inrix_data/processed_data/trips_usa_tx_202202_wk2"
file = "trips_device_od.parquet"


#note: section loads file above
#----- applies additional processing
#----- processing should be added to procedure
df_trips_od = here::here(folder, file) %>%
  arrow::read_parquet() %>%
  data.table::data.table() %>%
  .[, !c("segment_id", "flag_direction")] %>%
  unique() %>%
  .[,`:=`(start_date = as_datetime(start_date))] %>%
  #note: about ordering_trip sequences
  .[order(device_id.x, start_date)] %>%
  .[,`:=`(trip_seq = rowid(device_id.x))] %>%
  dt_fill(CROSS_NM, flag_seq, id = device_id.x, .direction = "down") #%>% #filling down probs works for entering as of right now
  # .[device_id.x == '0002bd6a9141d2d3d7d132c332fa1622',]

#make: index of just trips that are entering
df_sample_entering = df_trips_od %>%
  .[flag_seq == "entering" &
      trip_seq == 1]

#spot_checking_tabular==========================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#check: dupe trip_ids
df_trips_od %>%
  # .[, !c("segment_id", "flag_direction")] %>% unique() %>%
  .[,.(count = .N), by = .(trip_id)] %>%
  .[,.(freq = .N), by = .(count)] %>%
  .[order(count)]

df_trips_od %>%
  .[, !c("segment_id", "flag_direction")] %>% unique() %>%
  .[,.(count = .N), by = .(trip_id)]  %>%
  .[,head(.SD, 2), by = .(count)]

df_trips_od %>%
  .[trip_id == "fc569cfc1d41aaa4f2605ab23d3b4f40",] %>%
  .[, !c("segment_id", "flag_direction")] %>%
  unique()


df_trips_od %>%
  .[,.(count = .N), by = .(trip_id)] %>%
  .[,.(freq = .N), by = .(count)] %>%
  .[order(count)]

#check: dupe device_ids
df_trips_od %>%
  .[,.(count = .N), by = .(device_id.x)] %>%
  .[,.(freq = .N), by = .(count)] %>%
  .[order(count)]

df_trips_od %>%
  .[,.(count = .N), by = .(device_id.x)]  %>%
  .[,head(.SD, 2), by = .(count)] %>%
  .[count == 6,]

df_sepcific_trip = df_trips_od %>%
  # .[device_id.x == "008be0e1fea28462ca1f71c5465e26c4",] %>%
  .[device_id.x == "0002bd6a9141d2d3d7d132c332fa1622",] %>%
  # . == "0002bd6a9141d2d3d7d132c332fa1622 "
  .[,`:=`(start_date = as_datetime(start_date))] %>%
  .[order(start_date)] %>%
  .[,`:=`(trip_seq = .I), by = .(device_id.x)]


df_sepcific_trip %>%
  .[,`:=`(geometry = paste0("LINESTRING (", start_lon, " ", start_lat, ",  ", end_lon, " ", end_lat, ")") %>%
            as.character()
  )] %>%
  sf::st_as_sf(wkt = "geometry", crs = 4326) %>%
  mapview::mapview(zcol = "trip_seq")



#spatial_mapping_entering_only==================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#make: subset of singular crossing location
df_trips_od_enter = df_trips_od %>%
  .[device_id.x %in% unique(df_sample_entering$device_id.x)] %>%
  .[CROSS_NM == "World Trade Bridge",]

#linestrings of trips
#note: not very helpful
df_trips_od_enter_sp = df_trips_od_enter %>%
  .[,`:=`(geometry = paste0("LINESTRING (", start_lon, " ", start_lat, ",  ", end_lon, " ", end_lat, ")") %>%
          as.character()
)] %>%
  sf::st_as_sf(wkt = "geometry", crs = 4326)

df_trips_od_enter_sp %>%
  mapview::mapview(zcol = "trip_seq", alpha = .1)

#make: spatial layer of points
#----- first and end point of firstr trip
#----- last point of subsequent trips
temp_map = bind_rows(
  df_trips_od_enter %>%
    .[trip_seq == 1, ] %>%
    rename(lat = start_lat, lon = start_lon) %>%
    select(!c(starts_with("start_l"), starts_with("end_l"))) %>%
    mutate(flag_point_type = "start")
  ,df_trips_od_enter %>%
    .[trip_seq == 1, ] %>%
    rename(lat = end_lat, lon = end_lon) %>%
    select(!c(starts_with("start_l"), starts_with("end_l"))) %>%
    mutate(flag_point_type = "end")
  ,df_trips_od_enter %>%
    .[trip_seq != 1, ] %>%
    rename(lat = end_lat, lon = end_lon) %>%
    select(!c(starts_with("start_l"), starts_with("end_l"))) %>%
    mutate(flag_point_type = "end")
) %>%
  sf::st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
  st_jitter(.0001) %>%
  group_by(flag_point_type_1 = flag_point_type) %>%
  group_map(~{
    x =.x
    label = unique(x$flag_point_type)
    mapview::mapview(x, layer.name = label)
  })  %>%
  reduce(`+`)







df_trips_od_enter_sp_point = df_trips_od_enter %>%
  group_by(device_id.x) %>%
  mutate(
    lat = case_when(trip_seq == min(trip_seq)~start_lat, T~end_lat)
    ,lon = case_when(trip_seq == min(trip_seq)~start_lon, T~end_lon)
    ,flag_trip_seq = trip_seq == 1) %>%
  ungroup() %>%
  sf::st_as_sf(coords = c("lon", "lat"), crs = 4326)

df_trips_od_enter_sp_point %>%
  st_jitter(.0005) %>%
  mapview::mapview(zcol = "trip_seq", alpha = .05)

df_trips_od_enter %>%
  .[trip_seq != 1, ] %>%
  sf::st_as_sf(coords = c("end_lon", "end_lat"), crs = 4326) %>%
  st_jitter(.0005) %>%
  mapview::mapview(zcol = "trip_seq", alpha = .05)



