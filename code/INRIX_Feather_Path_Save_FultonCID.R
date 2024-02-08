library(feather)


rm(list = ls()) #clears stored variables

#########
## convert CSV to feather for fast processing
dir <- list()
dir[[1]]  = "C:/Users/guerrerose/Desktop/INRIX Data Fulton/Paths"
dir[[2]]  = "C:/Users/guerrerose/Desktop/INRIX Data Fulton/Paths"
dir[[3]]  = "C:/Users/guerrerose/Desktop/INRIX Data Fulton/Paths"
dir[[4]]  = "C:/Users/guerrerose/Desktop/INRIX Data Fulton/Paths"
dir[[5]]  = "C:/Users/guerrerose/Desktop/INRIX Data Fulton/Paths"

dirtraj = "C:/Users/guerrerose/Desktop/INRIX Data Fulton/Paths"

name <- list()
name[[1]] = "trajs_Fulton CID_201907_04_1.csv"
name[[2]] = "trajs_Fulton CID_201907_04_2.csv"
name[[3]] = "trajs_Fulton CID_201907_08_1.csv"
name[[4]] = "trajs_Fulton CID_201907_08_2.csv"
name[[5]] = "trajs_Fulton CID_201910_11.csv"

nameF <- list()
nameF[[1]] = "trajs_Fulton CID_201907_04_1.feather"
nameF[[2]] = "trajs_Fulton CID_201907_04_2.feather"
nameF[[3]] = "trajs_Fulton CID_201907_08_1.feather"
nameF[[4]] = "trajs_Fulton CID_201907_08_2.feather"
nameF[[5]] = "trajs_Fulton CID_201910_11.feather"


for (i in 3:length(dir)) {
  setwd(dir[[i]]) #sets user directory
  Traj   <- read.csv(file=name[[i]],header=FALSE,sep=",")
  setwd(dirtraj) #sets user directory
  write_feather(Traj, nameF[[i]])
  rm(Traj)
}
  

