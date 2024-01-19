#預估文字客服
#新增星期+平/假日
library(forecast)
library(dplyr)
library(lubridate)
library(zoo)
library(stringr)
#匯入資料
a1 <- read.csv("C:/Users/jasoncchen/Desktop/history.csv",header = T)
#轉換成日期型態
a1$Date <- ymd(a1$Date)
#篩選至次月月底，以便預估
a2 <- subset(a1, a1$Date <= "2022-09-30")
#轉換資料值，建立因子矩陣以便模型可辨識星期與平假日
a2$Weekday <- str_replace_all(a2$Weekday, pattern = "星期一", replacement = "1")
a2$Weekday <- str_replace_all(a2$Weekday, pattern = "星期二", replacement = "2")
a2$Weekday <- str_replace_all(a2$Weekday, pattern = "星期三", replacement = "3")
a2$Weekday <- str_replace_all(a2$Weekday, pattern = "星期四", replacement = "4")
a2$Weekday <- str_replace_all(a2$Weekday, pattern = "星期五", replacement = "5")
a2$Weekday <- str_replace_all(a2$Weekday, pattern = "星期六", replacement = "6")
a2$Weekday <- str_replace_all(a2$Weekday, pattern = "星期日", replacement = "7")
a2$Weekday.Hoilday_3 <- str_replace_all(a2$Weekday.Hoilday_3, pattern = "平日", replacement = "1")
a2$Weekday.Hoilday_3 <- str_replace_all(a2$Weekday.Hoilday_3, pattern = "假日", replacement = "2")
x1 <- cbind(weekday = model.matrix(~as.factor(a2$Weekday)),
            holiday = model.matrix(~as.factor(a2$Weekday.Hoilday_3)))
x2 <- x1[,-c(1,8)]
colnames(x2) <- c("Sat","Sun","Mon","Tue","Wed","Thu","Nor")
#篩選歷史資料
a3 <- subset(a2, a2$Date>="2022-05-01" & a2$Date <= "2022-08-21")
#建立ts物件，7天為一個循環
a4 <- ts(a3$Chat, frequency=7)
#建模， x2[486:598,]其中數字代表篩選的日期順序
m1 <- auto.arima(a4, xreg = x2[486:598,],stepwise = F,trace = T,stationary = T,ic = c("aic"))
# 預估7月，x2[599:638,] 其中數字代表預估的日期順序
x3 <- forecast(m1,xreg = x2[599:638,])
#取出預估值
x3$mean


#預估前線話務
library(forecast)
library(dplyr)
library(lubridate)
library(zoo)
library(stringr)
#匯入資料
a1 <- read.csv("C:/Users/jasoncchen/Desktop/history.csv",header = T)
#轉換成日期型態
a1$Date <- ymd(a1$Date)
a2 <- subset(a1, a1$Date <= "2022-09-30")
#轉換資料值
a2$Weekday <- str_replace_all(a2$Weekday, pattern = "星期一", replacement = "1")
a2$Weekday <- str_replace_all(a2$Weekday, pattern = "星期二", replacement = "2")
a2$Weekday <- str_replace_all(a2$Weekday, pattern = "星期三", replacement = "3")
a2$Weekday <- str_replace_all(a2$Weekday, pattern = "星期四", replacement = "4")
a2$Weekday <- str_replace_all(a2$Weekday, pattern = "星期五", replacement = "5")
a2$Weekday <- str_replace_all(a2$Weekday, pattern = "星期六", replacement = "6")
a2$Weekday <- str_replace_all(a2$Weekday, pattern = "星期日", replacement = "7")
a2$Weekday.Hoilday_3 <- str_replace_all(a2$Weekday.Hoilday_3, pattern = "平日", replacement = "1")
a2$Weekday.Hoilday_3 <- str_replace_all(a2$Weekday.Hoilday_3, pattern = "假日", replacement = "2")
x1 <- cbind(weekday = model.matrix(~as.factor(a2$Weekday)),
            holiday = model.matrix(~as.factor(a2$Weekday.Hoilday_3)))
x2 <- x1[,-c(1,8)]
colnames(x2) <- c("Sat","Sun","Mon","Tue","Wed","Thu","Nor")
a3 <- subset(a2, a2$Date>="2022-05-01" & a2$Date <= "2022-8-21")
#建立ts物件
a4 <- ts(a3$Call, frequency=7)
#建模
m1 <- auto.arima(a4, xreg = x2[486:598,],stepwise = F,trace = T,stationary = T,ic = c("aic"))
# 預估次月
x3 <- forecast(m1,xreg = x2[599:638,])
x3$mean