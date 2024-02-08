library(feather)
library(data.table)
library(purrr)


rm(list = ls()) #clears stored variables

dir = "C:/Users/guerrerose/Desktop/INRIX Data Fulton/Paths"
setwd(dir) #sets user directory

H   <- read.csv(file="TripBulkReportTrajectoriesHeaders.csv",header=FALSE,sep=",")

## Read the Traj Files
nameF <- list()
nameF[[1]] = "trajs_Fulton CID_201907_04_1.feather"
nameF[[2]] = "trajs_Fulton CID_201907_04_2.feather"
nameF[[3]] = "trajs_Fulton CID_201907_08_1.feather"
nameF[[4]] = "trajs_Fulton CID_201907_08_2.feather"
nameF[[5]] = "trajs_Fulton CID_201910_11.feather"

name <- list()
name[[1]] = "trajs_Fulton CID_201907_04_1.csv"
name[[2]] = "trajs_Fulton CID_201907_04_2.csv"
name[[3]] = "trajs_Fulton CID_201907_08_1.csv"
name[[4]] = "trajs_Fulton CID_201907_08_2.csv"
name[[5]] = "trajs_Fulton CID_201910_11.csv"

###trip data
Trips   <- read.csv(file="Trips_subset.csv",header=FALSE,sep=",")
colnames(Trips) <- Trips[2,]
Trips   <- Trips[3:nrow(Trips),]
Trips_fromCID = Trips[!is.na(Trips$O_Fulton_CID),]

################################
## Count all trucks in the file
CS <- list()  #count simple
for (i in 1:length(nameF)) {
  TJ = read_feather(nameF[[i]])
  colnames(TJ) = H
  CS[[i]] = as.data.frame(table(TJ$SegmentId))  # Count road volume
  rm(TJ)
}


V  = rbind(CS[[1]],CS[[2]],CS[[3]],CS[[4]])
CCS = aggregate(x = V$Freq,    by = list(V$Var1), FUN = sum)

write.csv(CCS, "OUT_VOl.csv")

################################
## Count all trucks in the file
C <- list()
for (i in 1:length(nameF)) {
  TJ = read_feather(nameF[[i]])
  colnames(TJ) = H
  TripId_s = unlist(TJ[TJ$SegmentId == Select_link[1] | TJ$SegmentId == Select_link[2],"TripId"]) #select trips that pass through selected links
  TJ_s     = TJ[TJ$TripId %in% TripId_s ,]  #TJ_s     = TJ[is.element(TJ$TripId, TripId_s) ,]
  a = as.data.frame(table(TJ_s$SegmentId))  # Count road volume
  C[[i]] = a
  rm(TJ_s)
}

#combine volumes

V  = rbind(C[[1]],C[[2]],C[[3]],C[[4]])
CC = aggregate(x = V$Freq,    by = list(V$Var1), FUN = sum)


write.csv(CC, "OUT_VOL_SelectLink.csv")

################################
## Count trucks to/from FIB
dir = "C:/Users/guerrerose/Desktop/INRIX Data Fulton/Paths"
setwd(dir) #sets user directory

C <- list()
for (i in 1:length(nameF)) {
  #TJ = read_feather(nameF[[i]])
  TJ   <- read.csv(file=name[[i]],header=FALSE,sep=",")
  colnames(TJ) = H
  TJ_s     = TJ[TJ$TripId %in% Trips_fromCID$TripId ,]  #TJ_s     = TJ[is.element(TJ$TripId, TripId_s) ,]
  a = as.data.frame(table(TJ_s$SegmentId))  # Count road volume
  C[[i]] = a
  rm(TJ_s)
}

#combine volumes

V  = rbind(C[[1]],C[[2]],C[[3]],C[[4]],C[[5]])
CC = aggregate(x = V$Freq,    by = list(V$Var1), FUN = sum)


write.csv(CC, "OUT_VOL_FromCID.csv")


#########'
# Identify routes using FIB to pass interchange 

## Count trucks to/from FIB
dir = "C:/Users/guerrerose/Desktop/INRIX Data Fulton/Paths"
setwd(dir) #sets user directory


C <- list()
for (i in 1:length(nameF)) {
  #TJ = read_feather(nameF[[i]])
  TJ   <- read.csv(file=name[[i]],header=FALSE,sep=",")
  colnames(TJ) = H
  TJ$SegmentId = ifelse(substr(TJ$SegmentId, 1, 1)=="-", substr(TJ$SegmentId, 2, nchar(TJ$SegmentId)), TJ$SegmentId)
  
  
  TripId_sO = TJ[TJ$SegmentId %in% "9239113_0","TripId"]   #exit ramp
  TripId_sD = TJ[TJ$SegmentId %in% "287426163_2","TripId"]  #entrance ramp
  TripId_s = intersect(TripId_sO,TripId_sD)
  
  TJ_s     = TJ[TJ$TripId %in% TripId_s,]  #TJ_s     = TJ[is.element(TJ$TripId, TripId_s) ,]
  a = as.data.frame(table(TJ_s$SegmentId))  # Count road volume
  C[[i]] = a
  rm(TJ_s)
}

#combine volumes

V  = rbind(C[[1]],C[[2]],C[[3]],C[[4]],C[[5]])
CC1 = aggregate(x = V$Freq,    by = list(V$Var1), FUN = sum)


write.csv(CC1, "OUT_VOL_Shortcut_Eastbound.csv")

## Count trucks to/from FIB
dir = "C:/Users/guerrerose/Desktop/INRIX Data Fulton/Paths"
setwd(dir) #sets user directory


C <- list()
for (i in 1:length(nameF)) {
  #TJ = read_feather(nameF[[i]])
  TJ   <- read.csv(file=name[[i]],header=FALSE,sep=",")
  colnames(TJ) = H
  TJ$SegmentId = ifelse(substr(TJ$SegmentId, 1, 1)=="-", substr(TJ$SegmentId, 2, nchar(TJ$SegmentId)), TJ$SegmentId)
  
  
  TripId_sO = TJ[TJ$SegmentId %in% "746011041_0","TripId"]   #exit ramp
  TripId_sD = TJ[TJ$SegmentId %in% "89481673_1","TripId"]  #entrance ramp
  TripId_s = intersect(TripId_sO,TripId_sD)
  
  TJ_s     = TJ[TJ$TripId %in% TripId_s,]  #TJ_s     = TJ[is.element(TJ$TripId, TripId_s) ,]
  a = as.data.frame(table(TJ_s$SegmentId))  # Count road volume
  C[[i]] = a
  rm(TJ_s)
}

#combine volumes

V  = rbind(C[[1]],C[[2]],C[[3]],C[[4]],C[[5]])
CC2 = aggregate(x = V$Freq,    by = list(V$Var1), FUN = sum)


write.csv(CC2, "OUT_VOL_Shortcut_Westbound.csv")
