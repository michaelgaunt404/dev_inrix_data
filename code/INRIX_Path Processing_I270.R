library(feather)
library(data.table)
library(purrr)



rm(list = ls()) #clears stored variables

dir = "C:/Users/guerrerose/Desktop/INRIX Data 270 TR/Trajs"
setwd(dir) #sets user directory

H   <- read.csv(file="TripBulkReportTrajectoriesHeaders.csv",header=FALSE,sep=",")

nameF <- list()
nameF[[1]] = "trajs_jan.feather"
nameF[[2]] = "trajs_jul.feather"
nameF[[3]] = "trajs_mar.feather"
nameF[[4]] = "trajs_oct.feather"

### select link
Select_link = c('82574884_0','82574882_0')


C <- list()
for (i in 1:length(nameF)) {
  
  TJ = read_feather(nameF[[i]])
  colnames(TJ) = H
  C[[i]] = table(TJ$SegmentId)  # Count road volume
  rm(TJ)
}



C <- list()
for (i in 1:length(nameF)) {
  
  TJ = read_feather(nameF[[i]])
  colnames(TJ) = H

  TripId_s = unlist(TJ[TJ$SegmentId == Select_link[1] | TJ$SegmentId == Select_link[2],"TripId"])
  TJ_s     = TJ[TJ$TripId %in% TripId_s ,]  #TJ_s     = TJ[is.element(TJ$TripId, TripId_s) ,]
  
  C[[i]] = table(TJ_s$SegmentId)  # Count road volume
  rm(TJ_s)
}








#combine volumes

a = C[[2]]
a$ID = rownames(a)

cbind.colnames(add,to=NULL,deparse.level = 1)
C1 = setDT(as.matrix(C[[1]]), keep.rownames = TRUE)[]

V  = rbind(C[[1]],C[[2]],C[[3]],C[[4]])
CC = table(V$Var1)



write.csv(CC, "OUT_VOL.csv")



Speed_avg = aggregate(TJ$CrossingSpeedKph, by = list(TJ$SegmentId),FUN = mean)