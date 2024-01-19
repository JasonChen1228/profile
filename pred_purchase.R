library(DBI) 
library(stringr) 
library(dplyr) 
library(lubridate) 
library(forecast) 
library(prophet) 
library(zoo) 
library(caret) 
library(reshape2) 
library(keyring) 
library(RPostgreSQL) 
library(purrr) 
setwd("/home/rf_0344_jason/") 
if(keyring_is_locked()){   keyring_unlock(password = "12345678") } 
msconn <- dbConnect(odbc::odbc(),                     
                    Driver = "ODBC Driver 17 for SQL SERVER",                     
                    Server = "10.0.130.225",                     
                    Database = "DataTeam",                     
                    UID = "jason_chen",                     
                    PWD = key_get("db_connect", "jason"),                     
                    Port = 1433) 
drv <- dbDriver("PostgreSQL") 
posconn <- dbConnect(drv, user = "tableau", 
                     password = "P@SSWORD", 
                     dbname = "digital_center",                       
                     host = "rf-postgre-rpa.cjxcxnblv1qd.ap-northeast-1.rds.amazonaws.com") 
tryCatch({   
g1 = as.POSIXlt(Sys.time())   ## send message to MSSQL ##   
job_msg = data.frame(task_name = "predict_for_week",                        
                     task_code = "predict_week.R",                        
                     task_status = 0,                        
                     start_time = force_tz(Sys.time(), "UTC"))   
dbWriteTable(msconn,                
             name = "JOB_MESSAGE",                
             row.names = F,                
             header = TRUE,                
             value = job_msg,                
             append = T)   
pred_item <- dbGetQuery(msconn, "SELECT QC FROM DataTeam.dbo.purchase_item_test")    
pred_item <- paste0("'",pred_item$QC, "'")   
x1 <- pred_item[1]   
for(i in 2:length(pred_item)){x1 <- paste(x1, pred_item[i], sep = ",")}   
b7 <- Sys.Date()-1 # 執行日期   
b9 <- b7 - day(b7) # 前一個月   
b10 <- b9 - day(b9) # 前兩個月   
b11 <- b10 - day(b10) # 前三個月   
b5 <- paste(year(b9), month(b9), "10", sep = "-") # 前一個月10號之前有販售紀錄   
b6 <- c(paste0(year(b11), "_", month(b11)), paste0(year(b10), "_", month(b10)), paste0(year(b9),  "_", month(b9))) # 選取前三個月銷量   
b8 <- as.Date(paste(year(Sys.Date() %m+% months(1)), month(Sys.Date() %m+% months(1)), "01", sep = "-"))   
tts_display <- dbGetQuery(posconn, paste0('SELECT * FROM sales.completed_orders WHERE "sub_QC" IN (',x1,') AND real_sale_price >= 0 AND real_sale_qty > 0')) %>% 
  mutate(order_day = as.Date(as.character(order_day))) 
cat('\nfilter sales data:', as.character.POSIXt(Sys.time()),'\n')  
sell_date <- tts_display %>% group_by(sub_QC) %>% summarise(early_date = min(order_day)) %>% 
  filter(early_date < b5) %>% as.data.frame() 
y1 <- tts_display %>% group_by(year = year(order_day), month = month(order_day), sub_QC) %>% 
  summarise(sales = sum(real_sale_qty)) %>% mutate(com = paste(year, month, sep = "_")) %>% 
  dcast(sub_QC ~ com, value.var = "sales") %>%  mutate_all(~replace(., is.na(.), 0)) %>% 
  mutate(ta = select(., b6) %>% reduce(`+`)) %>% filter(ta > 0) %>% as.data.frame() 
sell_date <- inner_join(sell_date, y1, by = "sub_QC") %>% as.data.frame() 
cat('\ncreate qc start_end dataset:', as.character.POSIXt(Sys.time()),'\n') 
dates <- c() 
item_id <- c() 
t1 <- c() 
t2 <- c() 
for(i in 1:nrow(sell_date)){   
  t1 <- seq(from = sell_date[i,2], to = b8 - 1 ,by = 1)   
  dates <- c(dates, t1)    
  ndays <- (b8 - 1) - sell_date[i,2] + 1   
  t2 <- rep(sell_date[i,1], ndays)   
  item_id <- c(item_id, t2) } 
qc_date <- data.frame(timestamp = as.Date(dates), item_id = item_id) 
display_name <- distinct(tts_display[, c("sub_QC","platform_name")]) 
tts_undisplay <- qc_date %>% left_join(display_name, by = c("item_id" = "sub_QC"), multiple = "all") %>%    
  left_join(tts_display, by = c("item_id" = "sub_QC", "timestamp" = "order_day", "platform_name" = "platform_name"), multiple = "all") %>%    
  mutate_at(vars(real_sale_qty), ~ coalesce(., 0)) %>%    
  group_by(timestamp,item_id) %>% summarise(sales = sum(real_sale_qty)) %>% rename("target_value" = "sales") 
cat("\ncreate act start_end dataset:", as.character.POSIXt(Sys.time()),'\n') 
act <- dbGetQuery(msconn, "SELECT parent_brand, display_name, campaign, start_date, end_date FROM DataTeam.dbo.sales_campaign") %>%    
  rename("brand" = "parent_brand","display" = "display_name","activity" = "campaign","start" = "start_date","end" = "end_date") %>%   
  mutate(start = as.Date(start), end = as.Date(end)) %>% as.data.frame() 
act_date <- c() 
brand <- c() 
display <- c() 
activity <- c() 
for(i in 1:nrow(act)){   
  t1 <- seq(from = act$start[i], to = act$end[i], by = 1)   
  act_date <- c(act_date, t1)   
  ndays <- act$end[i] - act$start[i]+1   
  t2 <- rep(act$brand[i], ndays)   
  brand <- c(brand, t2)   
  t3 <- rep(act$display[i], ndays)   
  display <- c(display, t3)   
  t4 <- rep(act$activity[i], ndays)   
  activity <- c(activity, t4) } 
act1 <- data.frame(dates = as.Date(act_date), brands = brand, display = display, activity = activity) %>%    
  filter(str_detect(brands, pattern = c("Unilever|美國康寧|Philips|Kose|Tefal|金百利|統一藥妝|新秀麗|Alpecin|Lock|好來")) == T & display != "團購") 
brands <- data.frame(b1 = c("Unilever", "美國康寧", "Philips", "Kose", "Tefal", "金百利", "統一藥妝", "新秀麗", "Alpecin", "Lock", "好來"),                      
                     b2 = c("Unilever", "美國康寧", "PHILIPS 飛利浦", "KOSE-高絲", "Tefal法國特福.", "金百利", "統一藥品", "新秀麗", "沃膚", "高佳林", "Colgate")) 
for(i in 1:nrow(brands)){   
  act1[str_detect(act1$brands, pattern = brands[i,1]) == T, "brands"] <- brands[i,2] 
  } 
display <- data.frame(d1 = c("蝦皮","Yahoo","momo","PChome"), d2 = c("蝦皮Shopee","Yahoo購物","momo購物","PChome購物")) 
for(i in 1:nrow(display)){   
  act1[str_detect(act1$display, pattern = display[i,1]) == T, "display"] <- display[i,2] 
  } 
act1 <- act1 %>% mutate(act = 1) %>% select(dates, brands, display, act) %>% distinct() 
cat('\ncreate act dataframe succeed:', as.character.POSIXt(Sys.time()),'\n') 
qc_date1 <- qc_date %>% left_join(display_name, by = c("item_id" = "sub_QC"), multiple = "all") %>%    
  mutate(display_name = if_else(platform_name %in% c("蝦皮Shopee", "PChome購物", "Yahoo購物","momo購物") == F, "others", platform_name)) %>% distinct() 
act2 <- left_join(qc_date1, distinct(tts_display[,c("sub_QC", "parent_brand")]), by = c("item_id" = "sub_QC"), multiple = "all") %>%    
  left_join(act1, by = c("timestamp" = "dates", "parent_brand" = "brands", "platform_name" = "display")) %>%   
  mutate(act = if_else(is.na(act) == T, 0, act)) %>% group_by(timestamp, item_id) %>% summarise(act = sum(act)) %>%    
  mutate(act = if_else(act > 0, 1, act)) 
holiday <- read.csv("holiday.csv", header = T) %>% mutate(timestamp = as.Date(timestamp)) 
weekday <- read.csv("weekday.csv", header = T) %>% mutate(timestamp = as.Date(timestamp)) 
tts_undisplay1 <- tts_undisplay %>% left_join(act2, by = c( "timestamp" = "timestamp", "item_id" = "item_id")) %>%    
  left_join(holiday, by = "timestamp") %>% left_join(weekday, by = "timestamp") %>%    
  mutate(week = weekdays(timestamp), holiday = if_else(is.na(holiday) == 0, 1, 0),           
         weekday = if_else(is.na(weekday) == 0, 1, 0), week = if_else(week %in% c("Saturday","Sunday"), 1, 0),          
         com = paste(holiday, weekday, week), com1 = if_else(com %in% c("0 1 1","0 0 0"), 0, 1)) %>%    
  select(timestamp, item_id, target_value, act, com1) %>% rename("day_type" = "com1") %>%    
  mutate(month = month(timestamp), day = day(timestamp), weekday = wday(timestamp))%>% as.data.frame() 
tts_undisplay1$order <- 1:nrow(tts_undisplay1) 
t1 <- c() 
t2 <- c() 
t3 <- unique(tts_undisplay1$item_id) 
item_id1 <- c() 
item_id2 <- c() 
fore_arima1 <- c() 
fore_arima2 <- c() 
for(i in 1:length(t3)){   
  a1 <- as.matrix(tts_undisplay1[, c(4,5)])   
  x1 <- ts(tts_undisplay1[tts_undisplay1$item_id == t3[i] & tts_undisplay1$timestamp < b7, "target_value"], frequency = 7)   
  a2 <- tts_undisplay1[tts_undisplay1$item_id == t3[i] & tts_undisplay1$timestamp < b7, "order"]   
  x2 <- 1:length(a2)   
  if(sum(a1[a2,1]) > 0){     
    undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = F, ic = c("aic"))     
    a3 <- tts_undisplay1[tts_undisplay1$item_id == t3[i] & tts_undisplay1$timestamp > (b7-1) & tts_undisplay1$timestamp < b8, "order"]     
    t1 <- rep(t3[i], length(a3)) #item_id     
    item_id1 <- c(item_id1, t1)     
    t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict     
    fore_arima1 <- c(fore_arima1, t2)   }
  else{     
    a1 <- as.matrix(tts_undisplay1[, 5])     
    undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = F, ic = c("aic"))     
    a3 <- tts_undisplay1[tts_undisplay1$item_id == t3[i] & tts_undisplay1$timestamp > (b7-1) & tts_undisplay1$timestamp < b8, "order"]     
    t1 <- rep(t3[i], length(a3)) #item_id     
    item_id2 <- c(item_id2, t1)     
    t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict     
    fore_arima2 <- c(fore_arima2, t2)   } } 
cat('\ncreate pred_arima dataframe:', as.character.POSIXt(Sys.time()),'\n') 
pred_arima <- data.frame(item_id = c(item_id1, item_id2), pred_value = c(fore_arima1, fore_arima2)) 
pred_arima$date <- rep(seq.Date(from = as.Date(b7,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(b8-b7)), length(unique(pred_arima$item_id))) 
pred_arima <- pred_arima[pred_arima$date >= Sys.Date(),] 
pred_arima[is.na(pred_arima$pred_value), "pred_value"] <- 0 
pred_arima[pred_arima$pred_value < 0, "pred_value"] <- 0 
pred_arima <- pred_arima %>% group_by(item_id) %>% summarise(sales = sum(pred_value)) 
pred_arima$model <- rep("arima", nrow(pred_arima)) 
k2 <- distinct(tts_display[,c("sub_QC", "parent_brand")]) 
pred_arima <- left_join(pred_arima, k2, by = c("item_id" = "sub_QC"), multiple = "all") %>% as.data.frame() 
colnames(pred_arima) <- c("QC", "p_sale_qty", "model", "parent_brand") 
pred_arima <- pred_arima[,c("parent_brand", "QC", "p_sale_qty", "model")] 
pred_arima$pred_start_date <- rep(b7+1, nrow(pred_arima)) 
pred_arima$pred_end_date <- rep(b8-1, nrow(pred_arima)) 
pred_arima$end_date <- rep(b7-1, nrow(pred_arima)) 
pred_arima$p_sale_qty <- round(pred_arima$p_sale_qty, digits = 0) 
# ets 
item_id <- c() 
ets_ann <- c() 
t1 <- c() 
t2 <- c() 
t7 <- unique(tts_undisplay1$item_id) 
for(i in 1:length(t7)){   
  q1 <- ets(ts(tts_undisplay1[tts_undisplay1$item_id == t7[i] & tts_undisplay1$timestamp < b7, "target_value"], frequency = 7), model = "ANN")   
  t1 <- forecast(q1, as.numeric(b8-b7))[["mean"]][1:as.numeric(b8-b7)]   
  ets_ann <- c(ets_ann, t1)   
  t2 <- rep(t7[i], as.numeric(b8-b7))   
  item_id <- c(item_id, t2) } 
pred_ets <- data.frame(item_id = item_id, ets_ann = ets_ann) 
pred_ets$date <- rep(seq.Date(from = as.Date(b7,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(b8-b7)), length(unique(pred_ets$item_id))) 
pred_ets <- pred_ets[pred_ets$date >= Sys.Date(),] 
pred_ets[is.na(pred_ets$ets_ann), "ets_ann"] <- 0 
pred_ets[pred_ets$ets_ann < 0, "ets_ann"] <- 0 
pred_ets <- pred_ets %>% group_by(item_id) %>%   
  summarise(ets_ann = sum(ets_ann)) %>% as.data.frame() 
pred_ets$model <- rep("ets_ann",nrow(pred_ets)) 
pred_ets <- left_join(pred_ets, k2, by = c("item_id" = "sub_QC"), multiple = "all") %>% as.data.frame() 
pred_ets <- pred_ets[, c("parent_brand", "item_id", "ets_ann", "model")] 
colnames(pred_ets) <- c("parent_brand", "QC", "p_sale_qty", "model") 
pred_ets$pred_start_date <- rep(b7+1, nrow(pred_arima)) 
pred_ets$pred_end_date <- rep(b8-1, nrow(pred_ets)) 
pred_ets$end_date <- rep(b7-1, nrow(pred_ets)) 
pred_ets$p_sale_qty <- round(pred_ets$p_sale_qty, digits = 0) 
pred <- rbind(pred_arima, pred_ets)   
write.csv(pred, paste("data/test/",paste(paste("pred_for_week", Sys.Date(), sep = "_"), ".csv", sep = ""), sep = ""), row.names = F,fileEncoding = "big5")   
pred = pred %>%     
  mutate_at(names(which(sapply(pred, class) == 'character')),               
            ~enc2native(.))   
table_id = Id(schema = "dbo", table = "pred_week")   
rs1 = dbWriteTable(msconn,                      
                   name = table_id,                      
                   row.names = F,                      
                   header = TRUE,                      
                   value = pred,                      
                   append = T)   
cat('\nINSERT', nrow(pred),'data, succeed:', rs1,'\n')   
## END   
g2 = as.POSIXlt(Sys.time())   
cat('\nTime difference of ',difftime(g2, g1, units = c("mins")),' mins\n')   
## update JOB message ##   
rs = dbSendQuery(msconn,paste0("   
                               UPDATE DataTeam.dbo.JOB_MESSAGE   SET task_status = 1, end_time = '", force_tz(Sys.time(),"UTC"), "'   
                               WHERE task_name = '", job_msg$task_name, "'   
                               AND convert(varchar, start_time, 120) = '", job_msg$start_time, "'"))   
dbClearResult(rs)   
dbDisconnect(msconn) },
error = function(msg){   
  msg = msg %>% str_replace_all("'", "''")   
  dbSendQuery(msconn,               
              paste0(                 
                "UPDATE DataTeam.dbo.JOB_MESSAGE                
                SET task_status = 2, end_time = '", force_tz(Sys.time(),"UTC"), "',                
                err_msg = '", msg, "'                
                WHERE task_name = '", job_msg$task_name, "'                
                AND convert(varchar, start_time, 120) = '", 
                job_msg$start_time, "'"))   
  dbDisconnect(msconn) })



  
  
  
  
  
  
  



  
  
  
  
  
  
  
  
  
  
  
  
  
  
