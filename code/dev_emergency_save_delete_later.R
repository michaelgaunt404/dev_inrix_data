

data_location = "C:/Users/gauntm/Documents/temp/week/trips_usa_tx_202208_wk4/date=2023-11-13/reportId=166942/v1/data/trajs"

file_list = here(data_location) %>%
  list.files(pattern = "parquet")





timed_list = c(5000, 7500, 10000)

timed_list = c(100)

list_loop = list()
list_furrr = list()

for (i in 1:length(timed_list)){

  subset_list = file_list %>%
    sample(timed_list[i])

  # tictoc::tic()
  # for (j in 1:length(subset_list)){
  #   print(j)
  #
  #   tt = here(
  #     data_location
  #     ,subset_list[j]
  #   ) %>%
  #     arrow::read_parquet(
  #       col_select = c("trip_id", "device_id", "provider_id", "trajectories")
  #     ) %>%
  #     data.table() %>%
  #     .[trip_id %in% index_reduced_trips]
  # }
  # end = tictoc::toc()
  #
  # list_loop[i] = parse_number(tictoc::toc()$callback_msg)
  #
  # rm(tt)
  # gc()

  plan(multisession, workers = 6)

  tictoc::tic()
  with_progress({
    p <- progressor(steps = length(subset_list))
    print(length(subset_list))

    tm = subset_list %>%
      future_map(~{
        p()

        here(
          data_location
          ,.x
        ) %>%
          arrow::read_parquet(
            col_select = c("trip_id", "device_id", "provider_id", "trajectories")
          ) %>%
          data.table() %>%
          .[trip_id %in% index_reduced_trips] %>% #prefiltering_Step
          # .[, .(trip_id, device_id, provider_id, trajectories)] %>%
          .[,.(ids =

                 tryCatch({
                   trajectories[[1]][["solution_segments"]] %>%
                     map(~{.x[["segment_id"]] }) %>%
                     reduce(c)
                 },  error = function(e) {return(NA_character_)})

          )
          ,by = .(trip_id, device_id, provider_id)] %>%
          .[ids %in% index_texas_omsids, ] %>%
          .[,`:=`(file = .x)]
      })
  })
  end_furr = tictoc::toc()

  list_furrr[i] = parse_number(end_furr$callback_msg)
}

list_loop %>% unlist()
















with_progress({
  p <- progressor(steps = limit)

  temp_data_rr =
    list(
      file_list[c(1:limit)]
      ,c(1:length(file_list))[c(1:limit)]
    ) %>%
    furrr::future_pmap(
      ~{
        p()

        message(str_glue("Processing {.x} -- {.y}"))

        tryCatch({
          temp_data = here(
            data_location
            ,.x
          ) %>%
            arrow::read_parquet(
              col_select = c("trip_id", "device_id", "provider_id", "trajectories")
            ) %>%
            data.table() %>%
            .[trip_id %in% index_reduced_trips] %>% #prefiltering_Step
            # .[, .(trip_id, device_id, provider_id, trajectories)] %>%
            .[,.(ids =

                   tryCatch({
                     trajectories[[1]][["solution_segments"]] %>%
                       map(~{.x[["segment_id"]] }) %>%
                       reduce(c)
                   },  error = function(e) {return(NA_character_)})

            )
            ,by = .(trip_id, device_id, provider_id)] %>%
            .[ids %in% index_texas_omsids, ] %>%
            .[,`:=`(file = .x)]

          message("Done")


        },  error = function(e) {
          error_message <- paste("An error occurred:/n", e$message)

          return(NA)
        })

        return(
          temp_data
        )

      }, .progress = F) %>%
    rbindlist()
})

temp_time_duration_perFile_nofilter = Sys.time()-temp_time_start







x = file_list %>%
  sample(1)

sample = 10

plan(multisession, workers = 6)


with_progress({
  p <- progressor(steps = sample)

  bench = file_list %>%
    sample(sample) %>%
    future_map(~{
      p()

      tictoc::tic()
      temp_data = here(
        data_location
        ,x
      ) %>%
        arrow::read_parquet(
          col_select = c("trip_id", "device_id", "provider_id", "trajectories")
        )
      time_load = parse_number(tictoc::toc()$callback_msg)

      tictoc::tic()
      temp_data = temp_data %>% .[temp_data$trip_id %in% index_reduced_trips,]
      temp_data_reduced = parse_number(tictoc::toc()$callback_msg)

      tictoc::tic()
      temp_dt = temp_data %>% data.table()
      time_dt = parse_number(tictoc::toc()$callback_msg)

      tictoc::tic()
      temp_dt_reduced = temp_dt %>% .[trip_id %in% index_reduced_trips]
      time_dt_reduced = parse_number(tictoc::toc()$callback_msg)

      tictoc::tic()
      temp_dt_segmatch = temp_dt %>%
        .[,.(ids =

               tryCatch({
                 trajectories[[1]][["solution_segments"]] %>%
                   map(~{.x[["segment_id"]] }) %>%
                   reduce(c)
               },  error = function(e) {return(NA_character_)})

        )
        ,by = .(trip_id, device_id, provider_id)]

      print(temp_dt_segmatch)
      time_dt_segmatch = parse_number(tictoc::toc()$callback_msg)

      tictoc::tic()
      temp_dt_reduced_segmatch = temp_dt_reduced %>%
        .[,.(ids =

               tryCatch({
                 trajectories[[1]][["solution_segments"]] %>%
                   map(~{.x[["segment_id"]] }) %>%
                   reduce(c)
               },  error = function(e) {return(NA_character_)})

        )
        ,by = .(trip_id, device_id, provider_id)] %>%
        .[ids %in% index_texas_omsids, ]
      time_dt_reduced_segmatch = parse_number(tictoc::toc()$callback_msg)

      dt = data.frame(
        time_load = time_load
        ,temp_data_reduced = temp_data_reduced
        ,time_dt = time_dt
        ,time_dt_segmatch = time_dt_segmatch
        ,time_dt_reduced = time_dt_reduced
        ,time_dt_reduced_segmatch = time_dt_reduced_segmatch
      )

      return(dt)
    })

})



rbindlist(bench) %>%
  mutate(ttl_no_dt = time_load + temp_data_reduced
         ,ttl_dt = time_load + time_dt + time_dt_segmatch
         ,ttl_dt_reduced = time_load + time_dt + time_dt_reduced + time_dt_reduced_segmatch)






temp_data[4,4] %>% .[[1]]

temp_data %>%
  data.table() %>%
  .[,.(ids =

         tryCatch({
           trajectories[[1]][["solution_segments"]] %>%
             map(~{.x[["segment_id"]] }) %>%
             reduce(c)
         },  error = function(e) {return(NA_character_)})

  )
  ,by = .(trip_id, device_id, provider_id)] %>%
  .[ids %in% index_texas_omsids, ]



temp_data$trajectories
tryCatch({
  trajectories[[1]][["solution_segments"]] %>%
    map(~{.x[["segment_id"]] }) %>%
    reduce(c)
},  error = function(e) {return(NA_character_)})





df <- data_frame(
  x = 1,
  y = list(list(a = 1, b = 2))
)

df %>%
  dmap(y)

purrr::flatten(df)




