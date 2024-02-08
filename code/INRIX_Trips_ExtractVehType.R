library(feather)
library(data.table)
library(purrr)


rm(list = ls()) #clears stored variables

dir = "C:/Users/guerrerose/Desktop/INRIX Data 270 TR/Trips"
setwd(dir) #sets user directory

## Read trip files

Trips_1   <- read.csv(file="trips_jan.csv",header=FALSE,sep=",")
Trips_2   <- read.csv(file="trips_jul.csv",header=FALSE,sep=",")
Trips_3   <- read.csv(file="trips_mar.csv",header=FALSE,sep=",")
Trips_4   <- read.csv(file="trips_oct.csv",header=FALSE,sep=",")

Trips = rbind(Trips_1,Trips_2,Trips_3,Trips_4)

H  <- read.csv(file="TripBulkReportTripsHeaders.csv",header=FALSE,sep=",")

colnames(Trips) <- H

write.csv(Trips, "Trips_comb.csv")

Trips_data = aggregate(x = Trips$VehicleWeightClass,    by = list(Trips$TripId), FUN = max)

Trips_d = Trips[,c("TripDistanceMeters","TripMeanSpeedKph")]


D2_NAICSSUM = merge(Trips_data, Trips_d, by.x = c("Group.1"), by.y = c("TripDistanceMeters"),all.x = TRUE)


write.csv(Trips_vehweight, "Trips_vehweight.csv")
