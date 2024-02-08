install.packages("feather")

library(feather)


rm(list = ls()) #clears stored variables

#########
## convert CSV to feather for fast processing
dir <- list()
dir[[1]]  = "C:/Users/guerrerose/Desktop/INRIX Data 270 TR/trips_co_jan_2019/data/trajs"
dir[[2]]  = "C:/Users/guerrerose/Desktop/INRIX Data 270 TR/trips_co_jul_2019/data/trajs"
dir[[3]]  = "C:/Users/guerrerose/Desktop/INRIX Data 270 TR/trips_co_mar_2019/data/trajs"
dir[[4]]  = "C:/Users/guerrerose/Desktop/INRIX Data 270 TR/trips_co_oct_2019/data/trajs"
dirtraj = "C:/Users/guerrerose/Desktop/INRIX Data 270 TR/Trajs"

name <- list()
name[[1]] = "trajs_jan.csv"
name[[2]] = "trajs_jul.csv"
name[[3]] = "trajs_mar.csv"
name[[4]] = "trajs_oct.csv"

nameF <- list()
nameF[[1]] = "trajs_jan.feather"
nameF[[2]] = "trajs_jul.feather"
nameF[[3]] = "trajs_mar.feather"
nameF[[4]] = "trajs_oct.feather"


for (i in 1:length(dir)) {
  setwd(dir[[i]]) #sets user directory
  Traj   <- read.csv(file=name[[i]],header=FALSE,sep=",")
  setwd(dirtraj) #sets user directory
  write_feather(Traj, nameF[[i]])
  rm(Traj)
}
  



read_feather(file, col_select = NULL, as_data_frame = TRUE, ...)
