#install.packages(dplyr)
library(feather)
library(data.table)
library(purrr)
library(chron)
library(parsedate)
library(lubridate)
library(data.table)
library(dplyr)
library(gtools)



rm(list = ls()) #clears stored variables

dir = "C:/Users/guerrerose/Desktop/INRIX Data 270 TR/Trajs"
setwd(dir) #sets user directory

H   <- read.csv(file="TripBulkReportTrajectoriesHeaders.csv",header=FALSE,sep=",")

## Read the Traj Files
nameF <- list()
nameF[[1]] = "trajs_jan.feather"
nameF[[2]] = "trajs_jul.feather"
nameF[[3]] = "trajs_mar.feather"
nameF[[4]] = "trajs_oct.feather"

### Links to select
Select_link = c('82574884_0','82574882_0')

### I-270 Segments
setwd(dir)
S_I270_EB  <- read.csv(file="INRIX_segments_I270_EB.csv",header=TRUE,sep=",")
S_I270_WB  <- read.csv(file="INRIX_segments_I270_WB.csv",header=TRUE,sep=",")
S_I70_EB   <- read.csv(file="INRIX_segments_I70_EB.csv",header=TRUE,sep=",")
S_I70_WB   <- read.csv(file="INRIX_segments_I70_WB.csv",header=TRUE,sep=",")
S_I25_EB   <- read.csv(file="INRIX_segments_I25_NB.csv",header=TRUE,sep=",")
S_I25_WB   <- read.csv(file="INRIX_segments_I25_SB.csv",header=TRUE,sep=",")

# add road name
S_I270_EB$segment_type = "I270_EB"
S_I270_WB$segment_type = "I270_WB"
S_I70_EB$segment_type = "I70_EB"
S_I70_WB$segment_type = "I70_WB"
S_I25_EB$segment_type = "I25_EB"
S_I25_WB$segment_type = "I25_WB"

segments = rbind(S_I270_EB,S_I270_WB,S_I70_EB,S_I70_WB,S_I25_EB,S_I25_WB)

### Load teh veh type data
setwd("C:/Users/guerrerose/Desktop/INRIX Data 270 TR/Trips") #sets user directory
Veh_type <- read.csv(file="Trips_vehweight.csv",header=TRUE,sep=",")
colnames(Veh_type) = c("ID","TripId","VehicleWeightClass")

setwd(dir) #sets user directory

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
## Count all medium duty trucks passing through select link IDs
C <- list()
TripId_s <- list()
for (i in 1:length(nameF)) {
  TJ = read_feather(nameF[[i]])
  colnames(TJ) = H
  Veh_type
  TJ <- merge(TJ, Veh_type, by.x = c('TripId'), by.y = c('TripId'), all.x = TRUE)
  TJ <- TJ[TJ$VehicleWeightClass==3,]
  TripId_s[[i]] = TJ[TJ$SegmentId %in% segments$seg_id,"TripId"]  
  #TripId_s[[i]] = unlist(TJ[TJ$SegmentId == Select_link[1] | TJ$SegmentId == Select_link[2],"TripId"]) #select trips that pass through selected links
  #TJ_s     = TJ[TJ$TripId %in% TripId_s[[i]] ,]  #TJ_s     = TJ[is.element(TJ$TripId, TripId_s) ,]
  #a = as.data.frame(table(TJ_s$SegmentId))  # Count road volume
  #C[[i]] = a
  rm(TJ)
}

#combine volumes

V  = rbind(C[[1]],C[[2]],C[[3]],C[[4]])
CC = aggregate(x = V$Freq,    by = list(V$Var1), FUN = sum)

TripId_s_comb  = unique(rbind(as.matrix(TripId_s[[1]]),as.matrix(TripId_s[[2]]),as.matrix(TripId_s[[3]]),as.matrix(TripId_s[[4]])))
save.image(file='tripid_med_temp_2.24.21') 
#write.csv(CC, "OUT_VOL_MEDIUM_TRUCKS_PASS-I270.csv")
write.csv(TripId_s_comb, "OUT_TRIPID_MEDIUM_TRUCKS_PASS-I270.csv")

#+++++++++++++++++++++++++++++++
################################
#+++++++++++++++++++++++++++++++
## Automated path volume generation

OD   <- read.csv(file="INPUT_OD_Analysis.csv",header=TRUE,sep=",")
for (j in 1:3) {
  C <- list()
  TripId_s <- list()  
  for (i in 1:length(nameF)) {
    TJ = read_feather(nameF[[i]])
    colnames(TJ) = H
    TJ <- merge(TJ, Veh_type, by.x = c('TripId'), by.y = c('TripId'), all.x = TRUE)
    TJ <- TJ[TJ$VehicleWeightClass==3|TJ$VehicleWeightClass==2,]
    #ss = segments[segments$segment_type=="I270_WB",]
    TJ$SegmentId = ifelse(substr(TJ$SegmentId, 1, 1)=="-", substr(TJ$SegmentId, 2, nchar(TJ$SegmentId)), TJ$SegmentId)
    
    TripId_sO = TJ[TJ$SegmentId %in% OD[j,"O"],"TripId"]  
    TripId_sD = TJ[TJ$SegmentId %in% OD[j,"D"],"TripId"]   
    TripId_s = intersect(TripId_sO,TripId_sD)
    #TripId_s[[i]] = unlist(TJ[TJ$SegmentId == Select_link[1] | TJ$SegmentId == Select_link[2],"TripId"]) #select trips that pass through selected links
    TJ_s     = TJ[TJ$TripId %in% TripId_s ,]  #TJ_s     = TJ[is.element(TJ$TripId, TripId_s) ,]
    a = as.data.frame(table(TJ_s$SegmentId))  # Count road volume
    C[[i]] = a
    rm(TJ)
  }
  
  #combine volumes
  
  V  = rbind(C[[1]],C[[2]],C[[3]],C[[4]])
  CC = aggregate(x = V$Freq,    by = list(V$Var1), FUN = sum)
  
  write.csv(CC, OD[j,"Name"])
}
#combine volumes


#+++++++++++++++++++++++++++++++
################################
#+++++++++++++++++++++++++++++++
## automated trip OD matrix generation, by time of day, exclude weekends

OD   <- read.csv(file="INRIX_Segments_OD_File.csv",header=TRUE,sep=",")
window_vec_O = unique(OD$Origin_Seg_ID)
window_vec_D = unique(OD$Dest_Seg_ID)
window_vec   = union(window_vec_O,window_vec_D)

TJ_out <- list()
DAYS <- list()

for ( i in 2:length(nameF)) {

  
  TJ = read_feather(nameF[[i]])
  colnames(TJ) = H
  TJ <- merge(TJ, Veh_type, by.x = c('TripId'), by.y = c('TripId'), all.x = TRUE)
  #TJ <- TJ[TJ$VehicleWeightClass==3|TJ$VehicleWeightClass==2,] #select any veh types
  
  TJ$VehType  = TJ$VehicleWeightClass
  
  TJ$SegmentId = ifelse(substr(TJ$SegmentId, 1, 1)=="-", substr(TJ$SegmentId, 2, nchar(TJ$SegmentId)), TJ$SegmentId) #remove directional minus
  
  TJ_ss = TJ[TJ$SegmentId %in% window_vec ,] #extract only trajectory records involving segment ODs
  
  # CREATE 15 MIN EPOCH
  # remove last z character
  TJ_ss$CrossingStartDateUtc_noZ = substr(TJ_ss$CrossingStartDateUtc,1,nchar(TJ_ss$CrossingStartDateUtc)-1)
  TJ_ss$CrossingEndDateUtc_noZ   = substr(TJ_ss$CrossingEndDateUtc,  1,nchar(TJ_ss$CrossingStartDateUtc)-1)
  
  TJ_ss$CrossingStartDate_UTC = as.POSIXlt(TJ_ss$CrossingStartDateUtc_noZ, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC") #convert to date time
  TJ_ss$CrossingEndDate_UTC   = as.POSIXlt(TJ_ss$CrossingEndDateUtc_noZ,   format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")
  
  TJ_ss$CrossingStartDate_MST = with_tz(TJ_ss[,'CrossingStartDate_UTC'], "MST") #change from UTC to MST
  TJ_ss$CrossingEndDate_MST   = with_tz(TJ_ss[,'CrossingEndDate_UTC'] , "MST")    
  
  #TJ_ss$CrossingStartDate_MST = with_tz(TJ_ss[,'CrossingStartDate_UTC'][[1]], "MST") #change from UTC to MST
  #TJ_ss$CrossingEndDate_MST   = with_tz(TJ_ss[,'CrossingEndDate_UTC'][[1]]  , "MST")
  
  TJ_ss$CrossingStartDate_15m = round((hour(TJ_ss$CrossingStartDate_MST)*60 + minute(TJ_ss$CrossingStartDate_MST))/15,digits = 0) #create 15 minute epochs
  TJ_ss$CrossingEndDate_15m   = round((hour(TJ_ss$CrossingEndDate_MST)*60   + minute(TJ_ss$CrossingEndDate_MST))/15  ,digits = 0)
  
  TJ_ss$CrossingStartDate_1hr = hour(TJ_ss$CrossingStartDate_MST) #create 15 minute epochs
  TJ_ss$CrossingEndDate_1hr   = hour(TJ_ss$CrossingEndDate_MST)
  
  TJ_ssw = TJ_ss[ !(wday(TJ_ss$CrossingStartDate_MST)==1 | wday(TJ_ss$CrossingStartDate_MST)==7) ,]  # exclude weekends
  
  TJ_ssw$year  = year(TJ_ssw$CrossingStartDate_MST)   # exclude holidays
  TJ_ssw$month = month(TJ_ssw$CrossingStartDate_MST)   
  TJ_ssw$day   = day(TJ_ssw$CrossingStartDate_MST)
  TJ_sswh = TJ_ssw[ !((TJ_ssw$month==1&TJ_ssw$day==1) | (TJ_ssw$month==7&TJ_ssw$day==4) | (TJ_ssw$month==11&TJ_ssw$day==28) | (TJ_ssw$month==11&TJ_ssw$day==29) | (TJ_ssw$month==12&TJ_ssw$day==25))  ,]
  
  TJ_sswh = TJ_sswh[!is.na(TJ_sswh$TripId),]
  
  DAYS[[i]] = length(unique(TJ_sswh$year*10^10 + TJ_sswh$month*10^5 + TJ_sswh$day))
  TJ_out[[i]] = TJ_sswh
  rm("TJ")
  rm("TJ_ss")
  rm("TJ_ssw")
  rm("TJ_sswh")
  
}

TJ_sswh  = rbind(TJ_out[[1]],TJ_out[[2]],TJ_out[[3]],TJ_out[[4]])
DD       = rbind(DAYS[[1]],DAYS[[2]],DAYS[[3]],DAYS[[4]])
save.image(file='tempsave.4.19.ODprocess.RData') 


out_vec = data.frame(matrix(0, nrow = 1, ncol = 72))

out_vec_n = c("0_1","1_1","2_1","3_1","4_1","5_1","6_1","7_1","8_1","9_1","10_1","11_1","12_1","13_1","14_1","15_1","16_1","17_1","18_1","19_1","20_1","21_1","22_1","23_1","0_2","1_2","2_2","3_2","4_2","5_2","6_2","7_2","8_2","9_2","10_2","11_2","12_2","13_2","14_2","15_2","16_2","17_2","18_2","19_2","20_2","21_2","22_2","23_2")
colnames(out_vec) = out_vec_n

OUT = data.frame(matrix(0, nrow = nrow(OD), ncol = 72))
colnames(OUT) = out_vec_n

OUT2 = data.frame(matrix(0, nrow = 72, ncol =  2))

OUT2$X1 = c(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)
OUT2$X2 = c(1,1,1,1,1,1,1,1,1,1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,2,2,2,2,2,2,2,2,2,2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,3,3,3,3,3,3,3,3,3,3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3)

C <- list()
TripId_s <- list()  
for (j in 1:nrow(OD)) {
  
  TripId_sO = TJ_sswh[TJ_sswh$SegmentId == OD[j,"Origin_Seg_ID"],"TripId"]  #get tripIDs that go through both
  TripId_sD = TJ_sswh[TJ_sswh$SegmentId == OD[j,"Dest_Seg_ID"]  ,"TripId"]     
  TripId_s = intersect(TripId_sO, TripId_sD)  #only get tripIDs that go through both
  TripId_s = TripId_s[!is.na(TripId_s)]
  
  TJ_sswhs     = TJ_sswh[TJ_sswh$TripId %in% TripId_s ,]  #get trajectories of trips that pass through ODs
  
  TJ_sswhs_O = TJ_sswhs[TJ_sswhs$SegmentId == OD[j,"Origin_Seg_ID"],]  #get trajectories of trips entering O, that also pass through D
  
  if (nrow(TJ_sswhs_O)==0) {
    p = setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("Group.1", "Group.2", "x"))

  } else {
    p = aggregate(TJ_sswhs_O$TripId, by = list(TJ_sswhs_O$CrossingStartDate_1hr,  TJ_sswhs_O$VehType), FUN = function(x) length(unique(x)))
  }
  
  OUT2 <- merge(OUT2, p, by.x = c('X1','X2'), by.y = c('Group.1','Group.2'), all.x = TRUE)
  
  # 
  # p$hr_veh = paste0(as.character(p$Group.1),"_",as.character(p$Group.2)) 
  # 
  # pt <- as.data.frame(t(p), stringsAsFactors = FALSE)
  # colnames(pt) = t(p$hr_veh)
  # pt = pt[c("x"),]
  # 
  # 
  # pt2 <- mutate_all(pt, function(x) as.numeric(as.character(x)))
  # 
  # w = colSums(smartbind(out_vec, pt2, fill=0, sep=':', verbose=FALSE))
  # 
  # OUT[j,] = w
  # 
  # TJ <- merge(TJ, Veh_type, by.x = c('TripId'), by.y = c('TripId'), all.x = TRUE)
  # 
  # 
}


#combine volumes
OUT3 = t(OUT2)
OUT3[is.na(OUT3)] = 0

OUT4 = cbind(rbind(c("Hour","Hour"),c("Veh Type","Veh Type"),OD),OUT3)

write.csv(OUT4,"OUT_OD_VOL2.csv", row.names=FALSE,col.names=FALSE)



#combine volumes
################################
## Count all heavy duty trucks passing through select link IDs
C <- list()
TripId_s <- list()
for (i in 1:length(nameF)) {
  TJ = read_feather(nameF[[i]])
  colnames(TJ) = H
  Veh_type
  TJ <- merge(TJ, Veh_type, by.x = c('TripId'), by.y = c('TripId'), all.x = TRUE)
  TJ <- TJ[TJ$VehicleWeightClass==3|TJ$VehicleWeightClass==2,]
  tv = c("628120919_0","800506260_0")
  #ss = segments[segments$segment_type=="I270_WB",]
  TripId_s[[i]] = TJ[TJ$SegmentId %in% tv,"TripId"]  
  #TripId_s[[i]] = unlist(TJ[TJ$SegmentId == Select_link[1] | TJ$SegmentId == Select_link[2],"TripId"]) #select trips that pass through selected links
  TJ_s     = TJ[TJ$TripId %in% TripId_s[[i]] ,]  #TJ_s     = TJ[is.element(TJ$TripId, TripId_s) ,]
  a = as.data.frame(table(TJ_s$SegmentId))  # Count road volume
  C[[i]] = a
  rm(TJ)
}

#combine volumes

V  = rbind(C[[1]],C[[2]],C[[3]],C[[4]])
CC = aggregate(x = V$Freq,    by = list(V$Var1), FUN = sum)

write.csv(CC, "OUT_VOL_871-3469_Truck.csv")

################################

################################
## Count all heavy duty trucks passing through select link IDs
C <- list()
TripId_s <- list()
for (i in 1:length(nameF)) {
  TJ = read_feather(nameF[[i]])
  colnames(TJ) = H
  Veh_type
  TJ <- merge(TJ, Veh_type, by.x = c('TripId'), by.y = c('TripId'), all.x = TRUE)
  TJ <- TJ[TJ$VehicleWeightClass==3|TJ$VehicleWeightClass==2,]
  tv = c("628120919_0","320769161_0")
  #ss = segments[segments$segment_type=="I270_WB",]
  TripId_s[[i]] = TJ[TJ$SegmentId %in% tv,"TripId"]  
  #TripId_s[[i]] = unlist(TJ[TJ$SegmentId == Select_link[1] | TJ$SegmentId == Select_link[2],"TripId"]) #select trips that pass through selected links
  TJ_s     = TJ[TJ$TripId %in% TripId_s[[i]] ,]  #TJ_s     = TJ[is.element(TJ$TripId, TripId_s) ,]
  a = as.data.frame(table(TJ_s$SegmentId))  # Count road volume
  C[[i]] = a
  rm(TJ)
}

#combine volumes

V  = rbind(C[[1]],C[[2]],C[[3]],C[[4]])
CC = aggregate(x = V$Freq,    by = list(V$Var1), FUN = sum)

write.csv(CC, "OUT_VOL_871-3474_Truck.csv")

################################
################################
################################
## Count all heavy duty trucks passing through select link IDs
C <- list()
TripId_s <- list()
for (i in 1:length(nameF)) {
  TJ = read_feather(nameF[[i]])
  colnames(TJ) = H
  Veh_type
  TJ <- merge(TJ, Veh_type, by.x = c('TripId'), by.y = c('TripId'), all.x = TRUE)
  TJ <- TJ[TJ$VehicleWeightClass==3 | TJ$VehicleWeightClass==2,]
  ss = segments[segments$segment_type=="I270_WB",]
  TripId_s[[i]] = TJ[TJ$SegmentId %in% ss$seg_id,"TripId"]  
  #TripId_s[[i]] = unlist(TJ[TJ$SegmentId == Select_link[1] | TJ$SegmentId == Select_link[2],"TripId"]) #select trips that pass through selected links
  TJ_s     = TJ[TJ$TripId %in% TripId_s[[i]] ,]  #TJ_s     = TJ[is.element(TJ$TripId, TripId_s) ,]
  a = as.data.frame(table(TJ_s$SegmentId))  # Count road volume
  C[[i]] = a
  rm(TJ)
}

#combine volumes

V  = rbind(C[[1]],C[[2]],C[[3]],C[[4]])
CC = aggregate(x = V$Freq,    by = list(V$Var1), FUN = sum)

#TripId_s_comb  = unique(rbind(as.matrix(TripId_s[[1]]),as.matrix(TripId_s[[2]]),as.matrix(TripId_s[[3]]),as.matrix(TripId_s[[4]])))
#save.image(file='tripid_heavy_temp_2.24.21') 

#write.csv(CC, "OUT_VOL_HEAVY_TRUCKS_PASS-I270.csv")
write.csv(CC, "OUT_VOL_WB270_TRUCK.csv")



load("tripid_med_temp_2.24.21.RData")

load("tripid_heavy_temp_2.24.21.RData")
