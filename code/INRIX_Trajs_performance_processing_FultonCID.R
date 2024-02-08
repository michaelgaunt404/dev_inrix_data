library(feather)
library(data.table)
library(purrr)
library(chron)
library(parsedate)
library(lubridate)
library(data.table)



rm(list = ls()) #clears stored variables

dir_path = "C:/Users/guerrerose/Desktop/INRIX Data Fulton/Paths"
dir_trip = "C:/Users/guerrerose/Desktop/INRIX Data Fulton/Trips"

setwd(dir_path) #sets user directory

H   <- read.csv(file="TripBulkReportTrajectoriesHeaders.csv",header=FALSE,sep=",")

# ## Read the Traj Files
# nameF <- list()
# nameF[[1]] = "trajs_Fulton_CID_201907_04_1.feather"
# nameF[[2]] = "trajs_Fulton_CID_201907_04_2.feather"
# nameF[[3]] = "trajs_Fulton_CID_201907_08_1.feather"
# nameF[[4]] = "trajs_Fulton_CID_201907_08_2.feather"
# nameF[[5]] = "trajs_Fulton_CID_201910_11.feather"

## Read the Traj Files
nameF <- list()
nameF[[1]] = "trajs_Fulton CID_201907_04_1.csv"
nameF[[2]] = "trajs_Fulton CID_201907_04_2.csv"
nameF[[3]] = "trajs_Fulton CID_201907_08_1.csv"
nameF[[4]] = "trajs_Fulton CID_201907_08_2.csv"
nameF[[5]] = "trajs_Fulton CID_201910_11.csv"
# 
# ###join data
# setwd(dir_path)
# Trips   <- read.csv(file="Trips_subset.csv",header=FALSE,sep=",")
# colnames(Trips) <- Trips[2,]
# Trips   <- Trips[3:nrow(Trips),]
# Trips_fromCID = Trips[!is.na(Trips$O_Fulton_CID),]
# colnames(Veh_type) = c("ID","TripId","VehicleWeightClass")


### segments in FIB
setwd(dir_path)
segments  <- read.csv(file="Segments_in_FultonCID.csv",header=TRUE,sep=",")
segments_fulton = segments[!is.na(segments$Fulton.CID),]

## Extract Only data on certain segments only
TJ_s <- list()  #count simple
for (i in 1:length(nameF)) {
  setwd(dir_path) #sets user directory
  # TJ = read_feather(nameF[[i]])
  TJ = read.csv(file=nameF[[i]],header=FALSE,sep=",")
  colnames(TJ) = H
  TJ_s[[i]]     = TJ[TJ$SegmentId %in% segments_fulton$seg_id,]  
  rm("TJ")
}

save.image(file='paths_inFulton_only3.17.RData') 
#load('paths_inFulton.RData')

TJ_s[[1]] = TJ_s[[1]][,c('TripId','CrossingStartDateUtc','CrossingEndDateUtc','CrossingSpeedKph','SegmentId')]
TJ_s[[2]] = TJ_s[[2]][,c('TripId','CrossingStartDateUtc','CrossingEndDateUtc','CrossingSpeedKph','SegmentId')]
TJ_s[[3]] = TJ_s[[3]][,c('TripId','CrossingStartDateUtc','CrossingEndDateUtc','CrossingSpeedKph','SegmentId')]
TJ_s[[4]] = TJ_s[[4]][,c('TripId','CrossingStartDateUtc','CrossingEndDateUtc','CrossingSpeedKph','SegmentId')]
TJ_s[[5]] = TJ_s[[5]][,c('TripId','CrossingStartDateUtc','CrossingEndDateUtc','CrossingSpeedKph','SegmentId')]

# TJ_s[[1]] = a1[a1$]

TJ_ss  = rbind(TJ_s[[1]],TJ_s[[2]],TJ_s[[3]],TJ_s[[4]])
rm("TJ_s")
TJ_ss = TJ_ss[!is.na(TJ_ss$CrossingSpeedKph),]

# CREATE 15 MIN EPOCH
# remove last z character
TJ_ss$CrossingStartDateUtc_noZ = substr(TJ_ss$CrossingStartDateUtc,1,nchar(TJ_ss$CrossingStartDateUtc)-1)
TJ_ss$CrossingEndDateUtc_noZ   = substr(TJ_ss$CrossingEndDateUtc,  1,nchar(TJ_ss$CrossingStartDateUtc)-1)

#TJ_ss$CrossingStartDate_LL = strptime(TJ_ss$CrossingStartDateUtc_noZ, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")
#TJ_ss$CrossingEndDate_LL   = strptime(TJ_ss$CrossingEndDateUtc_noZ, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")

TJ_ss$CrossingStartDate_UTC = as.POSIXlt(TJ_ss$CrossingStartDateUtc_noZ, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")
TJ_ss$CrossingEndDate_UTC   = as.POSIXlt(TJ_ss$CrossingEndDateUtc_noZ,   format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")

TJ_ss$CrossingStartDate_MST = with_tz(TJ_ss[,'CrossingStartDate_UTC'], "MST")
TJ_ss$CrossingEndDate_MST   = with_tz(TJ_ss[,'CrossingEndDate_UTC']  , "MST")

TJ_ss$CrossingStartDate_15m = round((hour(TJ_ss$CrossingStartDate_MST)*60 + minute(TJ_ss$CrossingStartDate_MST))/15,digits = 0)
TJ_ss$CrossingEndDate_15m   = round((hour(TJ_ss$CrossingEndDate_MST)*60   + minute(TJ_ss$CrossingEndDate_MST))/15  ,digits = 0)

#TJ_ss$CrossingStartDate_15m = round(hour(TJ_ss$CrossingStartDate_MST),digits = 0)
#TJ_ss$CrossingEndDate_15m   = round(hour(TJ_ss$CrossingEndDate_MST),digits = 0)
# exclude weekends
TJ_ssw = TJ_ss[ !(wday(TJ_ss$CrossingStartDate_MST)==1 | wday(TJ_ss$CrossingStartDate_MST)==7) ,]
rm("TJ_ss")

# exclude holidays
TJ_ssw$month = month(TJ_ssw$CrossingStartDate_MST)
TJ_ssw$day   = day(TJ_ssw$CrossingStartDate_MST)
TJ_sswh = TJ_ssw[ !((TJ_ssw$month==1&TJ_ssw$day==1) | (TJ_ssw$month==7&TJ_ssw$day==4) | (TJ_ssw$month==11&TJ_ssw$day==28) | (TJ_ssw$month==11&TJ_ssw$day==29) | (TJ_ssw$month==12&TJ_ssw$day==25))  ,]

# Veh type
TJ_sswhv <- TJ_sswh #   merge(TJ_sswh, Veh_type, by.x = c('TripId'), by.y = c('TripId'),all.x = TRUE)

rm("TJ_ss","TJ_ssw","TJ_sswh")
#save.image(file='tempsave_step2.RData') 

TJ_sswhv$CrossingSpeedMph = TJ_sswhv$CrossingSpeedKph*0.621371

# set max speed threshold
TJ_sswhv$CrossingSpeedMph_f = ifelse(TJ_sswhv$CrossingSpeedMph > 100, 100, TJ_sswhv$CrossingSpeedMph)

# calculate average speed
O_SPD_avg = aggregate(TJ_sswhv$CrossingSpeedMph_f, by = list(TJ_sswhv$SegmentId, TJ_sswhv$CrossingStartDate_15m), FUN = mean)
colnames(O_SPD_avg) = c("SegmentId","epoch_15min","MPH")

# calculate 75th percentile speed
O_SPD_75P = aggregate(TJ_sswhv$CrossingSpeedMph_f, by = list(TJ_sswhv$SegmentId, TJ_sswhv$CrossingStartDate_15m), FUN = function(x) quantile(x, probs = 0.75))
colnames(O_SPD_75P) = c("SegmentId","epoch_15min","MPH")

O_SPD_90P = aggregate(TJ_sswhv$CrossingSpeedMph_f, by = list(TJ_sswhv$SegmentId, TJ_sswhv$CrossingStartDate_15m), FUN = function(x) quantile(x, probs = 0.90))
colnames(O_SPD_90P) = c("SegmentId","epoch_15min","MPH")

# calculate 25th percentile speed
O_SPD_25P = aggregate(TJ_sswhv$CrossingSpeedMph_f, by = list(TJ_sswhv$SegmentId, TJ_sswhv$CrossingStartDate_15m), FUN = function(x) quantile(x, probs = 0.25))
colnames(O_SPD_25P) = c("SegmentId","epoch_15min","MPH")

O_SPD_05P = aggregate(TJ_sswhv$CrossingSpeedMph_f, by = list(TJ_sswhv$SegmentId, TJ_sswhv$CrossingStartDate_15m), FUN = function(x) quantile(x, probs = 0.05))
colnames(O_SPD_05P) = c("SegmentId","epoch_15min","MPH")

# count number of distinct vehicles in each measurement
TJ_idonly = TJ_sswhv[,c("TripId","SegmentId","CrossingStartDate_15m")]
DT <- data.table(TJ_idonly)
O_CNTD = DT[, .(number_of_distinct_orders = uniqueN(TripId)), by = list(TJ_sswhv$SegmentId, TJ_sswhv$CrossingStartDate_15m )]
colnames(O_CNTD) = c("SegmentId","epoch_15min","Number of Records")

O1 <- merge(O_SPD_avg, O_SPD_05P, by.x = c('SegmentId','epoch_15min'), by.y = c('SegmentId','epoch_15min'), all.x = TRUE)
O2 <- merge(       O1, O_SPD_25P, by.x = c('SegmentId','epoch_15min'), by.y = c('SegmentId','epoch_15min'), all.x = TRUE)
O3 <- merge(       O2, O_SPD_75P, by.x = c('SegmentId','epoch_15min'), by.y = c('SegmentId','epoch_15min'), all.x = TRUE)
O4 <- merge(       O3, O_SPD_90P, by.x = c('SegmentId','epoch_15min'), by.y = c('SegmentId','epoch_15min'), all.x = TRUE)
O5 <- merge(       O4, O_CNTD   , by.x = c('SegmentId','epoch_15min'), by.y = c('SegmentId','epoch_15min'), all.x = TRUE)
colnames(O5) = c("SegmentId","epoch_15min","MPH AVG","MPH 05P","MPH 25P","MPH 75P","MPH 90P","NUM RECORDS")

write.csv(O5, "O_SPEED_METRICS_EXPANDED.csv")