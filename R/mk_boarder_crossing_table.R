#makes border crossing table from list object

mk_boarder_crossing_table =
  function(folder = "//geoatfilpro1/cadd3/inrix_data/gis"
           ,file_nm = "extracted_border_crossing_locations_20240318.qs"){
    # mk_boarder_crossing_table()


    temp_df = here::here(
      folder
      ,file_nm
    ) %>%
      qs::qread()

    temp_df$border_locations_comb_pro %>%
      select(!c(geometry, roadname)) %>%
      unique() %>%
      group_by(seg_id) %>%
      mutate(flag_dupe = n()>1) %>%
      ungroup() %>%
      mutate(seg_id = case_when(
        flag_dupe & str_detect(flag_direction , "ex") ~ paste0("-", seg_id), T~seg_id
      )) %>%
      qs::qsave(
        here::here(folder, paste0("table_", file_nm))
      )
  }
