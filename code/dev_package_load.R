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

install.packages("devtools")

install.packages("magrittr")
library(magrittr)

install.packages("tidyverse")
library(tidyverse)

c("arrow", "clipr", "datapasta", "datatable", "furrr", "here", "leaflet"
  ,"mapedit", "mapview", "progressr", "qs", "sf", "tictoc") %>%
  map(install.packages)

#polars install----
Sys.setenv(NOT_CRAN = "true")
install.packages("polars", repos = "https://rpolars.r-universe.dev")
