#install.packages("Rmpfr")
library(base64enc)
library(sparklyr)
library(dplyr)
library(arrow)
library(doParallel)
library(foreach)
library(parallel) 
library(tidyr) 
library(feather) 
library(Rmpfr) #big number package

#furr, vectorize, cores or sessions,

rm(list = ls()) #clears stored variables
memory.limit(size=90000000)

nameDir <- list()
nameDir[[1]]  = "E:/NJ Truck Parking/Data - INRIX Trip Path/trip_paths_usa_nj_201904/date=2021-12-07/reportId=67791/v1/data/trajs"
nameDir[[2]]  = "E:/NJ Truck Parking/Data - INRIX Trip Path/trip_paths_usa_nj_202004/date=2021-12-07/reportId=67792/v1/data/trajs"
nameDir[[3]]  = "E:/NJ Truck Parking/Data - INRIX Trip Path/trip_paths_usa_nj_202104/date=2021-12-07/reportId=67793/v1/data/trajs"
nameDir[[4]]  = "E:/NJ Truck Parking/Data - INRIX Trip Path/trip_paths_usa_nj_202105/date=2021-12-07/reportId=67794/v1/data/trajs"
nameDir[[5]]  = "E:/NJ Truck Parking/Data - INRIX Trip Path/trip_paths_usa_nj_202106/date=2021-12-07/reportId=67795/v1/data/trajs"
nameDir[[6]]  = "E:/NJ Truck Parking/Data - INRIX Trip Path/trip_paths_usa_nj_202107/date=2021-12-07/reportId=67796/v1/data/trajs"
nameDir[[7]]  = "E:/NJ Truck Parking/Data - INRIX Trip Path/trip_paths_usa_nj_202108/date=2021-12-07/reportId=67797/v1/data/trajs"
nameDir[[8]]  = "E:/NJ Truck Parking/Data - INRIX Trip Path/trip_paths_usa_nj_202109/date=2021-12-07/reportId=67798/v1/data/trajs"

######################
n.cores <- detectCores()
clust <- makeCluster(n.cores-1)

# i could write code that extracts relevant traj data to .CSV, paralelized, but this is not great. Would be great to combine all the output 

## read only first folder, could through a forloop
files <- list.files(path = nameDir[[1]], pattern = "*.parquet", full.names = T) #works well for loop parallel
x <- foreach(i = 1:length(files))  %dopar% {
  read_parquet( files[i],  col_select = NULL,  as_data_frame = TRUE,  props = ParquetArrowReaderProperties$create(),  mmap = TRUE  ) %>% 
    unnest(cols = "trajectories") %>% unnest(cols = "solution_segments",names_repair = "unique") %>% 
    write.csv(paste0(files[i],".csv"),row.names=FALSE)
}

files <- list.files(path = nameDir[[1]], pattern = "*.parquet", full.names = T) #works well for loop parallel
aa=read_parquet( files[i],  col_select = NULL,  as_data_frame = TRUE,  props = ParquetArrowReaderProperties$create(),  mmap = TRUE  ) %>% 
  unnest(cols = "trajectories") %>% unnest(cols = "solution_segments",names_repair = "unique")

b=read_parquet( files[i],  col_select = NULL,  as_data_frame = TRUE,  props = ParquetArrowReaderProperties$create(),  mmap = TRUE  ) %>% 
  unnest(cols = "trajectories") 


######Convert Trip Parquets to .CSV 
for (i in 1:length(nameDir)) {
  setwd(nameDir[[i]])
  files <- list.files(path = nameDir[[i]], pattern = "*.parquet", full.names = T)
  sapply(files, read_parquet, simplify=FALSE) %>% bind_rows(.id = NULL) %>% write.csv("Trips.csv",row.names=FALSE)
}




files <- list.files(path = nameDir[[1]], pattern = "*.parquet", full.names = T)
a =  read_parquet( files[i],  col_select = NULL,  as_data_frame = TRUE,  props = ParquetArrowReaderProperties$create(),  mmap = TRUE  )


q=a[1,'trajectories']
 b = unnest(a[,], cols = "trajectories") %>% unnest(cols = "solution_segments",names_repair = "unique")
write.csv(b, paste0(files[i],".csv"),row.names=FALSE)

unnest(a[1,], cols = "trajectories") %>% unnest(cols = "solution_segments")




x <- foreach(i = 1:length(files))  %dopar% {
  read_parquet( files[i],  col_select = NULL,  as_data_frame = TRUE,  props = ParquetArrowReaderProperties$create(),  mmap = TRUE  ) %>% write.csv(paste0(files[i],".csv"),row.names=FALSE)
}


data.tables
RDS objects, native
keep ID and solution, and then run last unnest statement

######################
n.cores <- detectCores()
clust <- makeCluster(n.cores-1)

files <- list.files(path = nameDir[[1]], pattern = "*.parquet", full.names = T)
a = parSapply(clust, files, read_parquet, simplify=FALSE) %>% bind_rows(.id = NULL) #works very

stopCluster(clust)

###############
n.cores <- detectCores()
clust <- makeCluster(n.cores-1)

files <- list.files(path = nameDir[[1]], pattern = "*.parquet", full.names = T)
a = parSapply(clust, files, read_parquet, simplify=FALSE) %>% bind_rows(.id = NULL) #works very

stopCluster(clust)






x <- foreach(i = 1:length(nameDir))  %dopar% {
  files <- list.files(path = nameDir[[1]], pattern = "*.parquet", full.names = T)
  tbl <- sapply(files, read_parquet, simplify=FALSE) %>% bind_rows(.id = "id")
}



for (i in 1:length(nameDir)) {
  
  files <- list.files(path = nameDir[[1]], pattern = "*.parquet", full.names = T)
  a = sapply(files, read_parquet, simplify=FALSE) %>% bind_rows(.id = "id")

}
  
setwd(dir) #sets use


 




###
a = read_parquet(
  'part-00000-2c6b1c99-003a-4529-a97f-56653a8a2d14-c000.gz.parquet',
  col_select = NULL,
  as_data_frame = TRUE,
  props = ParquetArrowReaderProperties$create(),
  mmap = TRUE
  )

####################
# spark https://www.rdocumentation.org/packages/sparklyr/versions/1.7.5/topics/spark_read_parquet
sc <- spark_connect(master = "local", version = "2.3")
spark_tbl_handle  = spark_read_parquet(sc, "traj1", dir)
regular_df <- collect(spark_tbl_handle)
spark_disconnect(sc)