# =====================================================================
# CSE487/587
# Author: Prakash Natarajan
# Email: pn33@buffalo.edu
# =====================================================================


library(forecast)
library(fpp)

# filepath which has to changed for SLURM
# filePath = "/Users/prakashn/Documents/DIC/hw2/testing/"
filePath = "/gpfs/courses/cse587/spring2015/data/hw2/data/"
homeDir = "/user/pn33/"
# homeDir = "/Users/prakashn/Documents/DIC/hw2/"
fileList = list.files(filePath)


setwd(filePath)
trueList=c()
for(i in 1:length(fileList)) {
  textData=tryCatch(read.csv(file=fileList[i], header=T), error=function(e) {fileList[i]})
   if(!is.null(textData) && length(textData)==7) {
    if(nrow(textData)==754) {
      trueList=c(trueList, fileList[i])
    }
  }
}

finalList = fileList[fileList%in%trueList]

# compute the arima for the file list
arima_matrix = matrix(NA, 1, length(finalList))
hw_matrix = matrix(NA, 1, length(finalList))
lr_matrix = matrix(NA, 1, length(finalList))
for(i in 1:length(finalList)) {
  # originalFileName = paste(filePath, finalList[i], sep = "")
  # textData=read.csv(file=originalFileName, header=T)

  textData=tryCatch(read.csv(file=finalList[[i]], header=T),error=function(e) {print(e)}) 
  # convert txt data to time-series data, in day unit (DO NOT EDIT)
    tsData = ts(rev(textData$Adj.Close),start=c(2012, 1),frequency=365)
    
    # define train data (DO NOT EDIT)
    trainData = window(tsData, end=c(2014,14))
    
    # define test data (DO NOT EDIT)
    testData = window(tsData, start=c(2014,15))
               
    # MAE row vector (DO NOT EDIT)
    MAE = matrix(NA,1,length(testData))
    MAE_HW = matrix(NA,1,length(testData))
    MAE_LR = matrix(NA,1,length(testData))

    # apply ARIMA model (DO NOT EDIT)
    fitData = auto.arima(trainData, seasonal=FALSE, lambda=NULL, approximation=TRUE)
    # the other two models
    ### TO DO
    
    # apply forecast(DO NOT EDIT)
    forecastData = forecast(fitData, h=length(testData))

    for(j in 1:length(testData))
      {
        MAE[1,j] = abs(forecastData$mean[j] - testData[j])
      }
    arima_matrix[1, i] = sum(MAE[1,1:10])
    
    # apply forecast(DO NOT EDIT)
    forecastData = forecast(fitData, h=length(testData))

  for(j in 1:length(testData))
    {
      MAE[1,j] = abs(forecastData$mean[j] - testData[j])
    }
  arima_matrix[1, i] = sum(MAE[1,1:10])

  # HoltWinters computation
  fitData_HW = HoltWinters(trainData, beta=FALSE, gamma=FALSE)
  forecastData_HW = forecast(fitData_HW, h=length(testData))
  for(j in 1:length(testData))
    {
      MAE_HW[1,j] = abs(forecastData_HW$mean[j] - testData[j])
    }
  hw_matrix[1,i] = sum(MAE_HW[1,1:10])

  # #Linear regression
  mean = mean(testData)
  mean_matrix = matrix(mean, 1,length(trainData))
  mean_vector = as.vector(mean_matrix)
  fit_x=tslm(trainData~trend+season)
  forecastData_LR = forecast(fit_x, h=length(testData), newdata=trainData)
  for(j in 1:length(testData))
    {
      MAE_LR[1,j] = abs(forecastData_LR$mean[j] - testData[j])
    }
  lr_matrix[1,i] = sum(MAE_LR[1,1:10])
}
#go to home directory
setwd(homeDir)

# Convert to a vector
arima_vector = sapply(arima_matrix, c)
output_df = data.frame(finalList, arima_vector)
arima_output = output_df[order(output_df$arima_vector),] 
arima_head = head(arima_output, n=10)
print("arima_output :")
print(arima_head)
write.table(arima_head, "./arima.txt", sep="\t")

hw_vector = sapply(hw_matrix, c)
output_df = data.frame(finalList, hw_vector)
hw_output = output_df[order(output_df$hw_vector),] 
hw_head = head(hw_output, n=10)
write.table(hw_head, "./hw.txt", sep="\t")
print("hw output")
print(hw_head)

lr_vector = sapply(lr_matrix, c)
output_df = data.frame(finalList, lr_vector)
lr_output = output_df[order(output_df$lr_vector),] 
lr_head = head(lr_output, n=10)
write.table(lr_head, "./lr.txt", sep="\t")
print("lr output")
print(lr_head)

print("output has been written to a text file /user/pn33/ directory with names arima.txt, lr.txt, hw.txt")
jpeg("arima.jpg")
with(arima_head, plot(arima_vector, xlab="Company Index", ylab="MAE-SUM values",  main="ARIMA", type="p"))
with(arima_head, lines(arima_vector, type = "o", lw=2, col="green"))
with(arima_head, text(arima_vector, (sub("^([^.]*).*", "\\1", finalList)), cex=0.7, pos=1, col="black"))
graphics.off()

jpeg("hw.jpg")
with(hw_head, plot(hw_vector, xlab="Company Index", ylab="MAE-SUM values", main="Holt Winters", type="p"))
with(hw_head, lines(hw_vector, type="o", lw=2, col="green"))
with(hw_head, text(hw_vector, (sub("^([^.]*).*", "\\1", finalList)), cex=0.7, pos=1, col="black"))
graphics.off()

jpeg("lr.jpg")
with(lr_head, plot(lr_vector, xlab="Company Index", ylab="MAE-SUM values", main="Linear Regression", type="p"))
with(lr_head, lines(lr_vector, type="o", lw=2, col="green"))
with(lr_head, text(lr_vector, (sub("^([^.]*).*", "\\1", finalList)), cex=0.7, pos=1, col="black"))
graphics.off()
