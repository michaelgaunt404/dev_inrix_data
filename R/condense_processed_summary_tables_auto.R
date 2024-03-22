condense_processed_summary_tables_auto = function(border_xing_table, folder_root, cores){

  temp_dirs = list.files(folder_root) %>%
    .[!stringr::str_detect(., "mg")] %>%
    here::here(folder_root, .)

  temp_dirs %>%
    .[!str_detect(., "trips_usa_tx_202202_wk2")] %>%
    map(~{
      x = .x

      tryCatch({
        message(str_glue("{gauntlet::strg_make_space_2()}Condensing trips for the following folder:\n{x}"))

        temp_df = condense_processed_summary_tables(
          folder_location = x
          ,cores = cores
          ,border_xing_table = border_xing_table)

        message(str_glue("{nrow(temp_df)}"))

        message("Completed - now saving")

        arrow::write_parquet(
          temp_df
          ,here::here(
            x
            ,str_glue("condensed_summary_table.parquet")
          )
        )

      })

    })
}
