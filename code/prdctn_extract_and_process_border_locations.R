#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# This is script is used to extract border crossing locations.
#
# By: mike gaunt, michael.gaunt@wsp.com
#
# README: uses a fair amount of outside data objects for processing
#-------- this is the most up to date version
#
# *please use 80 character margins
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#library set-up=================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#content in this section should be removed if in production - ok for dev

library(arrow)
library(data.table)
library(gauntlet)
library(here)
library(sf)
library(tidyverse)
library(qs)

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

texas_boarder_locations = here::here(
  "//geoatfilpro1/cadd3/inrix_data/gis/border_locations"
  ,"CommercialVehicleInternationalBridges.shp") %>%
  sf::read_sf() %>%
  gauntletMap::st_quick_buffer(radius = 4000) %>%
  select(CROSS_NM)

mapview::mapview(texas_boarder_locations)

#main header====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
data_location = "//geoatfilpro1/cadd3/inrix_data/gis/USA_TX_OSM_20190201_segments_shapefile"

extracted_osm_border_xings = c("", 1, 2, 3, 4, 5) %>%
  paste0("SegmentReference", ., ".shp") %>%
  map(~{
    x = .x

    texas_temp = here(data_location, x) %>% read_sf()

    texas_temp_rm = texas_temp %>% st_filter(texas_boarder_locations)

    print(nrow(texas_temp_rm))

    return(texas_temp_rm)

    # texas_temp_rm %>% mapview::mapview(lwd = 5)

  }) %>%
  reduce(bind_rows)

test = mapview::mapview(
  extracted_osm_border_xings
  ,label = "roadname"
  # ,lwd = 5
  )

test_map = test@map

border_locations_exit = mapedit::selectFeatures(
  extracted_osm_border_xings
  # ,styleFalse = list(weight = 1)
  # ,styleTrue = list(weight = 20, color = "black")
  )

# border_locations_entry
# border_locations_exit

border_locations_comb = bind_rows(
  border_locations_entry %>%
    mutate(flag_direction = "entering")
  ,border_locations_exit %>%
    mutate(flag_direction = "exting")) %>%
  select(seg_id, roadname, flag_direction)

border_locations_comb_pro = texas_boarder_locations %>%
  st_join(border_locations_comb) %>%
  st_drop_geometry() %>%
  select(seg_id, CROSS_NM) %>%
  merge(border_locations_comb, .
        ,by = "seg_id")


border_locations_comb %>%
  st_join(texas_boarder_locations)


border_crossing_map = mapview::mapview(
  border_locations_comb_pro
  ,zcol = "flag_direction"
  ,lwd = 5) +
  mapview::mapview(
    texas_boarder_locations
    ,zcol = "CROSS_NM") +
  mapview::mapview(
    extracted_osm_border_xings)

border_crossing_map@map

list(
  border_locations_comb_pro = border_locations_comb_pro
  ,border_locations_entry = border_locations_entry
  ,border_locations_exit = border_locations_exit
  ,texas_boarder_locations = texas_boarder_locations
  ,extracted_osm_border_xings = extracted_osm_border_xings
) %>% qs::qsave(
  ., here::here(
    "//geoatfilpro1/cadd3/inrix_data/gis"
    ,"extracted_border_crossing_locations_20240318.qs"
  )
)


#main header====================================================================
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


