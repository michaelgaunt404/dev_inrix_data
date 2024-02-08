library(feather)
library(data.table)
library(purrr)
library(chron)
library(parsedate)
library(lubridate)
library(data.table)

#Rtools

rm(list = ls()) #clears stored variables

dir = "C:/Users/guerrerose/Desktop/INRIX Data 270 TR/Trajs"
setwd(dir)
H   <- read.csv(file="TripBulkReportTrajectoriesHeaders.csv",header=FALSE,sep=",")

## Read the Traj Files
nameF <- list()
nameF[[1]] = "trajs_jan.feather"
nameF[[2]] = "trajs_jul.feather"
nameF[[3]] = "trajs_mar.feather"
nameF[[4]] = "trajs_oct.feather"

## Read Veh. Type
setwd("C:/Users/guerrerose/Desktop/INRIX Data 270 TR/Trips") #sets user directory
Veh_type <- read.csv(file="Trips_vehweight.csv",header=TRUE,sep=",")
colnames(Veh_type) = c("ID","TripId","VehicleWeightClass")

### I-270 Segments
setwd(dir)
S_I270_EB  <- read.csv(file="INRIX_segments_I270_EB.csv",header=TRUE,sep=",")
S_I270_WB  <- read.csv(file="INRIX_segments_I270_WB.csv",header=TRUE,sep=",")
S_I70_EB   <- read.csv(file="INRIX_segments_I70_EB.csv",header=TRUE,sep=",")
S_I70_WB   <- read.csv(file="INRIX_segments_I70_WB.csv",header=TRUE,sep=",")
S_I25_EB   <- read.csv(file="INRIX_segments_I25_NB.csv",header=TRUE,sep=",")
S_I25_WB   <- read.csv(file="INRIX_segments_I25_SB.csv",header=TRUE,sep=",")
S_add      <- read.csv(file="INRIX_segments_rosellaadditions.csv",header=TRUE,sep=",")

# add road name
S_I270_EB$segment_type = "I270_EB"
S_I270_WB$segment_type = "I270_EB"
S_I70_EB$segment_type = "I70_EB"
S_I70_WB$segment_type = "I70_WB"
S_I25_EB$segment_type = "I25_EB"
S_I25_WB$segment_type = "I25_WB"
S_add$segment_type    = "add"

segments = rbind(S_I270_EB,S_I270_WB,S_I70_EB,S_I70_WB,S_I25_EB,S_I25_WB,S_add)

## Extract Only data on certain segments only
TJ_s <- list()  #count simple
for (i in 1:length(nameF)) {
  setwd(dir) #sets user directory
  TJ = read_feather(nameF[[i]])
  colnames(TJ) = H
  TJ_s[[i]]     = TJ[TJ$SegmentId %in% segments$seg_id,]  
  rm("TJ")
}
#save.image(file='tempsave83121.RData') 
#load("tempsave83121.RData")
TJ_ss  = rbind(TJ_s[[1]],TJ_s[[2]],TJ_s[[3]],TJ_s[[4]])

TJ_ss = TJ_ss[!is.na(TJ_ss$CrossingSpeedKph),]

# CREATE 15 MIN EPOCH
# remove last z character
TJ_ss$CrossingStartDateUtc_noZ = substr(TJ_ss$CrossingStartDateUtc,1,nchar(TJ_ss$CrossingStartDateUtc)-1)
TJ_ss$CrossingEndDateUtc_noZ   = substr(TJ_ss$CrossingEndDateUtc,  1,nchar(TJ_ss$CrossingStartDateUtc)-1)

#TJ_ss$CrossingStartDate_LL = strptime(TJ_ss$CrossingStartDateUtc_noZ, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")
#TJ_ss$CrossingEndDate_LL   = strptime(TJ_ss$CrossingEndDateUtc_noZ, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")

TJ_ss$CrossingStartDate_UTC = as.POSIXlt(TJ_ss$CrossingStartDateUtc_noZ, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")
TJ_ss$CrossingEndDate_UTC   = as.POSIXlt(TJ_ss$CrossingEndDateUtc_noZ,   format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")

TJ_ss$CrossingStartDate_MST = with_tz(TJ_ss[,'CrossingStartDate_UTC'][[1]], "MST")
TJ_ss$CrossingEndDate_MST   = with_tz(TJ_ss[,'CrossingEndDate_UTC'][[1]]  , "MST")

TJ_ss$CrossingStartDate_15m = round((hour(TJ_ss$CrossingStartDate_MST)*60 + minute(TJ_ss$CrossingStartDate_MST))/15,digits = 0)
TJ_ss$CrossingEndDate_15m   = round((hour(TJ_ss$CrossingEndDate_MST)*60   + minute(TJ_ss$CrossingEndDate_MST))/15  ,digits = 0)

# exclude weekends
TJ_ssw = TJ_ss[ !(wday(TJ_ss$CrossingStartDate_MST)==1 | wday(TJ_ss$CrossingStartDate_MST)==7) ,]

# exclude holidays
TJ_ssw$month = month(TJ_ssw$CrossingStartDate_MST)
TJ_ssw$day   = day(TJ_ssw$CrossingStartDate_MST)
TJ_sswh = TJ_ssw[ !((TJ_ssw$month==1&TJ_ssw$day==1) | (TJ_ssw$month==7&TJ_ssw$day==4) | (TJ_ssw$month==11&TJ_ssw$day==28) | (TJ_ssw$month==11&TJ_ssw$day==29) | (TJ_ssw$month==12&TJ_ssw$day==25))  ,]

# Veh type
TJ_sswhv <- merge(TJ_sswh, Veh_type, by.x = c('TripId'), by.y = c('TripId'),all.x = TRUE)

rm("TJ_ss","TJ_ssw","TJ_sswh")
#save.image(file='tempsave_step_08312021.RData') 
#load("tempsave_step_08312021.RData")

# exclude records with error code
TJ_sswhv = TJ_sswhv[!(TJ_sswhv$ErrorCodes=="101" | TJ_sswhv$ErrorCodes=="104" | TJ_sswhv$ErrorCodes=="103" | TJ_sswhv$ErrorCodes=="101,104"),]

TJ_sswhv$CrossingSpeedMph_f = TJ_sswhv$CrossingSpeedKph*0.621371

# set max speed threshold
TJ_sswhv$CrossingSpeedOverT = ifelse(TJ_sswhv$CrossingSpeedMph > 100, 1, 0)
TJ_sswhv = TJ_sswhv[TJ_sswhv$CrossingSpeedKph < 160.934 ,]

# calculate travel time
TJ_sswhv$CrossingCalcTT = (TJ_sswhv$LengthM/1000) / (TJ_sswhv$CrossingSpeedKph) *3600

# calculate average length
O_LEN_avg = aggregate(TJ_sswhv$LengthM, by = list(TJ_sswhv$SegmentId, TJ_sswhv$CrossingStartDate_15m, TJ_sswhv$VehicleWeightClass ), FUN = mean)
colnames(O_LEN_avg) = c("SegmentId","epoch_15min","VehicleWeightClass","LengthM")

# calculate average speed - harmonic 
O_SPD_avg = aggregate(TJ_sswhv$CrossingCalcTT, by = list(TJ_sswhv$SegmentId, TJ_sswhv$CrossingStartDate_15m, TJ_sswhv$VehicleWeightClass ), FUN = mean)
colnames(O_SPD_avg) = c("SegmentId","epoch_15min","VehicleWeightClass","TTs")
O_SPD_avg <- merge(O_SPD_avg, O_LEN_avg, by.x = c("SegmentId","epoch_15min","VehicleWeightClass"), by.y = c("SegmentId","epoch_15min","VehicleWeightClass"),all.x = TRUE)

O_SPD_avg$MPH =  (O_SPD_avg$LengthM/1000) / (O_SPD_avg$TTs/3600) * 0.621371

# calculate average speed - simple 
O_SPD_avg_simple = aggregate(TJ_sswhv$CrossingSpeedMph_f, by = list(TJ_sswhv$SegmentId, TJ_sswhv$CrossingStartDate_15m, TJ_sswhv$VehicleWeightClass ), FUN = mean)
colnames(O_SPD_avg) = c("SegmentId","epoch_15min","VehicleWeightClass","MPH")

# calculate 75th percentile speed
O_SPD_75P = aggregate(TJ_sswhv$CrossingSpeedMph_f, by = list(TJ_sswhv$SegmentId, TJ_sswhv$CrossingStartDate_15m, TJ_sswhv$VehicleWeightClass ), FUN = function(x) quantile(x, probs = 0.75))
colnames(O_SPD_75P) = c("SegmentId","epoch_15min","VehicleWeightClass","MPH")

# calculate 25th percentile speed
O_SPD_25P = aggregate(TJ_sswhv$CrossingSpeedMph_f, by = list(TJ_sswhv$SegmentId, TJ_sswhv$CrossingStartDate_15m, TJ_sswhv$VehicleWeightClass ), FUN = function(x) quantile(x, probs = 0.25))
colnames(O_SPD_25P) = c("SegmentId","epoch_15min","VehicleWeightClass","MPH")

# count number of distinct vehicles in each measurement
TJ_idonly = TJ_sswhv[,c("TripId","SegmentId","CrossingStartDate_15m","VehicleWeightClass")]
DT <- data.table(TJ_idonly)
O_CNTD = DT[, .(number_of_distinct_orders = uniqueN(TripId)), by = list(TJ_sswhv$SegmentId, TJ_sswhv$CrossingStartDate_15m, TJ_sswhv$VehicleWeightClass )]
colnames(O_CNTD) = c("SegmentId","epoch_15min","VehicleWeightClass","Number of Records")

# count number of distinct vehicles in each measurement
TJ_sswhvO = TJ_sswhv[TJ_sswhv$CrossingSpeedOverT==1,]
TJ_idonlyO = TJ_sswhvO[,c("TripId","SegmentId","CrossingStartDate_15m","VehicleWeightClass")]
DT <- data.table(TJ_idonlyO)
O_CNTD_O = DT[, .(number_of_distinct_orders = uniqueN(TripId)), by = list(TJ_sswhvO$SegmentId, TJ_sswhvO$CrossingStartDate_15m, TJ_sswhvO$VehicleWeightClass )]
colnames(O_CNTD_O) = c("SegmentId","epoch_15min","VehicleWeightClass","Number of Records")

O1 <- merge(O_SPD_avg, O_SPD_25P, by.x = c('SegmentId','epoch_15min','VehicleWeightClass'), by.y = c('SegmentId','epoch_15min','VehicleWeightClass'), all.x = TRUE)
O2 <- merge(       O1, O_SPD_75P, by.x = c('SegmentId','epoch_15min','VehicleWeightClass'), by.y = c('SegmentId','epoch_15min','VehicleWeightClass'), all.x = TRUE)
O3 <- merge(       O2, O_CNTD   , by.x = c('SegmentId','epoch_15min','VehicleWeightClass'), by.y = c('SegmentId','epoch_15min','VehicleWeightClass'), all.x = TRUE)
O4 <- merge(       O3, O_CNTD_O , by.x = c('SegmentId','epoch_15min','VehicleWeightClass'), by.y = c('SegmentId','epoch_15min','VehicleWeightClass'), all.x = TRUE)
colnames(O4) = c("SegmentId","epoch_15min","VehicleWeightClass","SECONDS","METERS","MPH AVG","MPH 25P","MPH 75P","NUM RECORDS","NUM RECORDS OVER THR")

write.csv(O4, "O_SPEED_METRICS_new10072021.csv")
