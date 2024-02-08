#install.packages()
library(feather)
library(data.table)
library(purrr)


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


### Define whole segments
setwd(dir)
S_I270_EB  <- read.csv(file="INRIX_segments_I270_EB.csv",header=TRUE,sep=",")
S_I270_WB  <- read.csv(file="INRIX_segments_I270_WB.csv",header=TRUE,sep=",")
S_I70_EB   <- read.csv(file="INRIX_segments_I70_EB.csv",header=TRUE,sep=",")
S_I70_WB   <- read.csv(file="INRIX_segments_I70_WB.csv",header=TRUE,sep=",")
S_I25_NB   <- read.csv(file="INRIX_segments_I25_NB.csv",header=TRUE,sep=",")
S_I25_SB   <- read.csv(file="INRIX_segments_I25_SB.csv",header=TRUE,sep=",")

# add road name
S_I270_EB$segment_type = "I270_EB"
S_I270_WB$segment_type = "I270_WB"
S_I70_EB$segment_type = "I70_EB"
S_I70_WB$segment_type = "I70_WB"
S_I25_NB$segment_type = "I25_NB"
S_I25_SB$segment_type = "I25_SB"

segments = rbind(S_I270_EB,S_I270_WB,S_I70_EB,S_I70_WB,S_I25_NB,S_I25_SB)
segments = segments[,c("seg_id","length_m","segment_type")]



################################
C <- list()
TripId_length <- list()
tripID_entrance_epoch_270 <-list()
###############################
for (i in 1:length(nameF)) {
  TJ = read_feather(nameF[[i]])                        # could try to convert to data table to help speed
  colnames(TJ) = H 
  TJ = TJ[,c("TripId","SegmentId","CrossingStartDateUtc")]                    # keep only two rows to reduce size
  TJ_s <- merge(TJ, segments, by.x = c("SegmentId"), by.y = c("seg_id"),all.x = TRUE) #merge file that describes if segments are selected segments
  TJ_ss = TJ_s[TJ_s$SegmentId %in% segments$seg_id,] # select only trip_IDs that pass through selected roads
  seg_len_ag = aggregate(TJ_ss$length_m, by = list(TJ_ss$TripId, TJ_ss$segment_type), FUN = sum) # calculate sum of length on key roads
  
  colnames(seg_len_ag) = c("TripId","segment_type","length_m")
  TripId_length[[i]] <- xtabs(length_m ~ TripId+segment_type, data=seg_len_ag)
  # write.csv(TJ_ss[TJ_ss$TripId == '0003df1eca8f301c45edb4f6f865121c',], "temp.csv")
  # seg_len_ag[seg_len_ag$TripId == '000079a10b1e15b91b4d066cdbb1cc9f', ]
  # write.csv(TJ_s[TJ_s$TripId =='0003df1eca8f301c45edb4f6f865121c',], "TJ.csv")

  rm(TJ)
  rm(TJ_s)
  rm(TJ_ss)
}

#combine volumes
V  = rbind(TripId_length[[1]],TripId_length[[2]],TripId_length[[3]],TripId_length[[4]])
W  = rbind(tripID_entrance_epoch_270[[1]],tripID_entrance_epoch_270[[2]],tripID_entrance_epoch_270[[3]],tripID_entrance_epoch_270[[4]])

#write.csv(CC, "OUT_VOL_MEDIUM_TRUCKS_PASS-I270.csv")
write.csv(V, "OUT_TRIPID_SEG_LENGTH.csv")



###################################
############### volume processing at segments

### Define I-270 volume segments
I_270_segments$seg_id = c("37355557_0","37355553_0","42084083_0","800597889_0","643623099_0","312153913_1")
I_270_segments$name = c("I-270 SB @ South of I-76 Interchange","I-270 NB @ South of I-76 Interchange","I-270 SB @ Vasquez Blvd","I-270 NB @ Vasquez Blvd","I-270 SB @ North of I-70 Interchange","I-270 NB @ North of I-70 Interchange")
TJ_out <- list()

for (i in 2:length(nameF)) {
  TJ = read_feather(nameF[[i]])                        # could try to convert to data table to help speed
  colnames(TJ) = H 
  TJ = TJ[,c("TripId","SegmentId","CrossingStartDateUtc")]                    # keep only two rows to reduce size
  TJ_s <- merge(TJ, I_270_segments, by.x = c("SegmentId"), by.y = c("seg_id"),all.x = TRUE) #merge file that describes if segments are selected segments
  TJ_ss = TJ_s[TJ_s$SegmentId %in% I_270_segments$seg_id,] # select only trip_IDs that pass through selected roads
  #seg_len_ag = aggregate(TJ_ss$length_m, by = list(TJ_ss$TripId, TJ_ss$segment_type), FUN = sum) # calculate sum of length on key roads
  
  #colnames(seg_len_ag) = c("TripId","segment_type","length_m")
  #TripId_length[[i]] <- xtabs(length_m ~ TripId+segment_type, data=seg_len_ag)
  # write.csv(TJ_ss[TJ_ss$TripId == '0003df1eca8f301c45edb4f6f865121c',], "temp.csv")
  # seg_len_ag[seg_len_ag$TripId == '000079a10b1e15b91b4d066cdbb1cc9f', ]
  # write.csv(TJ_s[TJ_s$TripId =='0003df1eca8f301c45edb4f6f865121c',], "TJ.csv")
  
  TJ_out[[i]] = TJ_ss
  rm(TJ_ss)
  rm(TJ_s)
  rm(TJ)
  
}

TJ_ss  = rbind(TJ_out[[1]],TJ_out[[2]],TJ_out[[3]],TJ_out[[4]])
save.image(file='tempsave.4.21.ODprocess.RData') 


# CREATE 15 MIN EPOCH
# remove last z character
TJ_ss$CrossingStartDateUtc_noZ = substr(TJ_ss$CrossingStartDateUtc,1,nchar(TJ_ss$CrossingStartDateUtc)-1)
TJ_ss$CrossingStartDate_UTC = as.POSIXlt(TJ_ss$CrossingStartDateUtc_noZ, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")
TJ_ss$CrossingStartDate_MST = with_tz(TJ_ss[,'CrossingStartDate_UTC'], "MST")
TJ_ss$CrossingStartDate_15m = round((hour(TJ_ss$CrossingStartDate_MST)*60 + minute(TJ_ss$CrossingStartDate_MST))/15,digits = 0)
TJ_ss$CrossingStartDate_1hr = hour(TJ_ss$CrossingStartDate_MST)
  
# exclude weekends
TJ_ssw = TJ_ss[ !(wday(TJ_ss$CrossingStartDate_MST)==1 | wday(TJ_ss$CrossingStartDate_MST)==7) ,]
  
# exclude holidays
TJ_ssw$month = month(TJ_ssw$CrossingStartDate_MST)
TJ_ssw$day   = day(TJ_ssw$CrossingStartDate_MST)
TJ_sswh = TJ_ssw[ !((TJ_ssw$month==1&TJ_ssw$day==1) | (TJ_ssw$month==7&TJ_ssw$day==4) | (TJ_ssw$month==11&TJ_ssw$day==28) | (TJ_ssw$month==11&TJ_ssw$day==29) | (TJ_ssw$month==12&TJ_ssw$day==25))  ,]
  
write.csv(TJ_ss, "OUT_I270_traj.csv")


#
entrance_epoch_270 = aggregate(TJ_sswh270$CrossingStartDate_15m, by = list(TJ_sswh270$TripId, TJ_sswh270$segment_type), FUN = min) # calculate sum of length on key roads
colnames(entrance_epoch_270) = c("TripId","segment_type","entrance")
tripID_entrance_epoch_270[[i]] <- xtabs(entrance ~ TripId+segment_type, data=entrance_epoch_270)
  


#combine volumes
V  = rbind(TripId_length[[1]],TripId_length[[2]],TripId_length[[3]],TripId_length[[4]])
W  = rbind(tripID_entrance_epoch_270[[1]],tripID_entrance_epoch_270[[2]],tripID_entrance_epoch_270[[3]],tripID_entrance_epoch_270[[4]])

#write.csv(CC, "OUT_VOL_MEDIUM_TRUCKS_PASS-I270.csv")
write.csv(V, "OUT_TRIPID_SEG_LENGTH.csv")


