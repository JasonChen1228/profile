library(DBI)
library(stringr)
library(dplyr)
library(lubridate)
library(forecast)
library(tseries)
library(prophet)
library(zoo)
library(caret)
library(reshape2)
library(keyring)
library(RPostgreSQL)
library(purrr)
msconn <- dbConnect(odbc::odbc(),
                    Driver = "ODBC Driver 17 for SQL SERVER",
                    Server = "10.0.130.225",
                    Database = "DataTeam",
                    UID = "jason_chen",
                    PWD = key_get("db_connect", "jason"),
                    Port = 1433)
drv <- dbDriver("PostgreSQL")
posconn <- dbConnect(drv, user = "tableau", password = "P@SSWORD", dbname = "digital_center", 
                     host = "rf-postgre-rpa.cjxcxnblv1qd.ap-northeast-1.rds.amazonaws.com")

g1 = as.character.POSIXt(Sys.time())
## send message to MSSQL ##
job_msg = data.frame(task_name = "pred_platform",
                     task_code = "pred_platform.R",
                     task_status = 0,
                     start_time = force_tz(Sys.time(), "UTC"))
dbWriteTable(msconn,
             name = "JOB_MESSAGE",
             row.names = F,
             header = T,
             value = job_msg,
             append = T)
# 四大平台近3個月有販售的商品次月銷售預估
# 最早販售日需早於上個月的10號、歷史銷售資料、合併促銷活動、平假日
# 預測日期
# p1 <- as.Date(paste(year(Sys.Date() %m+% months(2)), month(Sys.Date() %m+% months(2)), "01", sep = "-"))
# p2 <- Sys.Date()
p2 <- as.Date("2023-10-15")
p1 <- as.Date(paste(year(p2 %m+% months(2)), month(p2 %m+% months(2)), "01", sep = "-"))
p3 <- as.Date(paste0(year(p2 %m+% months(-3)), "-", month(p2 %m+% months(-3)), "-01")) # 近3個月
p4 <- as.Date(paste0(year(p2 %m+% months(-6)), "-", month(p2 %m+% months(-6)), "-01")) # 近6個月
p5 <- as.Date(paste0(year(p2 %m+% years(-1)), "-", month(p2), "-01")) # 同期月初
p6 <- as.Date(paste0(year(p5 %m+% months(1)), "-", month(p5 %m+% months(1)), "-01")) # 同期次月初
p7 <- as.Date(paste0(year(p2), "-", month(p2), "-01"))

# 產品資訊
prod <- dbGetQuery(msconn, "SELECT DISTINCT parent_brand, QC FROM DataTeam.dbo.ce_product")
cat('\nget sell qc:', as.character.POSIXt(Sys.time()),'\n')
# 取前3個月有銷量的QC排除新秀麗、Apivita、Nespresso、Valmont、金百利、美國Eagle Creek、AT美國旅行者、SHARP 夏普、Ecovacs
# sell_qc <- dbGetQuery(posconn, 
#                       paste0("SELECT DISTINCT",paste0('"',"QC",'"'),", platform_name
#                              FROM sales.completed_orders 
#                              WHERE platform_name IN ('蝦皮購物', 'Yahoo購物', 'PChome購物', 'momo購物') 
#                              AND order_day >= DATE_TRUNC('month', current_date - interval '3' month) 
#                              AND EXTRACT(MONTH  FROM order_day ) != EXTRACT(MONTH  FROM now() )
#                              AND parent_brand NOT IN ('Nespresso','新秀麗','Valmont','APIVITA','金百利','美國Eagle Creek','AT美國旅行者','SHARP 夏普','Ecovacs')")) %>% 
#   mutate(QC = case_when(str_detect(QC, pattern = "1000036323", negate = F) == T ~ "1000036323",
#                         str_detect(QC, pattern = "1000036322", negate = F) == T ~ "1000036322",
#                         str_detect(QC, pattern = "1000052879", negate = F) == T ~ "1000052879",
#                         T ~ QC)) %>% data.frame()


sell_qc <- dbGetQuery(posconn, 
                      paste0("SELECT DISTINCT",paste0('"',"QC",'"'),", platform_name
                             FROM sales.completed_orders 
                             WHERE platform_name IN ('蝦皮購物', 'Yahoo購物', 'PChome購物', 'momo購物') 
                             AND order_day >= DATE_TRUNC('month', date ",paste0("'",p2,"'")," - interval '3' month) 
                             AND EXTRACT(MONTH  FROM order_day ) != EXTRACT(MONTH  FROM TIMESTAMP ",paste0("'",p2,"'")," )
                             AND parent_brand NOT IN ('Nespresso','新秀麗','Valmont','APIVITA','金百利','美國Eagle Creek','AT美國旅行者','SHARP 夏普','Ecovacs','MEMEBOX美美箱','代運營專案','久光','LG 樂金','LEHO','固鋼','實體門市')")) %>% 
  mutate(QC = case_when(str_detect(QC, pattern = "1000036323", negate = F) == T ~ "1000036323",
                        str_detect(QC, pattern = "1000036322", negate = F) == T ~ "1000036322",
                        str_detect(QC, pattern = "1000052879", negate = F) == T ~ "1000052879",
                        T ~ QC)) %>% data.frame()

cat('\nget sales data:', as.character.POSIXt(Sys.time()),'\n')
# 歷史訂單排除新秀麗、Apivita、Nespresso、Valmont、金百利、美國Eagle Creek、AT美國旅行者、SHARP 夏普、Ecovacs
order <- dbGetQuery(posconn, 
                    paste0("SELECT DISTINCT",paste0('"',"QC",'"'),", order_id, order_day, platform_name,  sale_qty 
                           FROM sales.completed_orders 
                           WHERE platform_name IN ('蝦皮購物', 'Yahoo購物', 'PChome購物', 'momo購物')
                           AND parent_brand NOT IN ('Nespresso','新秀麗','Valmont','APIVITA','金百利','美國Eagle Creek','AT美國旅行者','SHARP 夏普','Ecovacs')")) %>% 
  mutate(order_day = as.Date(as.character(order_day)), 
         QC = case_when(str_detect(QC, pattern = "1000036323", negate = F) == T ~ "1000036323",
                        str_detect(QC, pattern = "1000036322", negate = F) == T ~ "1000036322",
                        str_detect(QC, pattern = "1000052879", negate = F) == T ~ "1000052879",
                        T ~ QC)) %>%
  filter(QC %in% sell_qc$QC) %>% as.data.frame()

cat('\nget min sell order day:', as.character.POSIXt(Sys.time()),'\n') # 四大平台商品最早販售日需早於前兩個月月初
# mo_sell_date <- order %>% 
#   filter(platform_name == "momo購物") %>% 
#   group_by(QC) %>% 
#   summarise(early_date = min(order_day)) %>% 
#   filter(QC %in% sell_qc[sell_qc$platform_name == "momo購物", "QC"] & 
#            early_date <= as.Date(format(Sys.Date()-months(2), "%Y-%m-01"))) %>%
#   as.data.frame() 
# 
# sp_sell_date <- order %>% 
#   filter(platform_name == "蝦皮購物") %>%
#   group_by(QC) %>% 
#   summarise(early_date = min(order_day)) %>% 
#   filter(QC %in% sell_qc[sell_qc$platform_name == "蝦皮購物", "QC"] & 
#            early_date <= as.Date(format(Sys.Date()-months(2), "%Y-%m-01"))) %>%
#   as.data.frame() 
# 
# pc_sell_date <- order %>% 
#   filter(platform_name == "PChome購物") %>%
#   group_by(QC) %>% 
#   summarise(early_date = min(order_day)) %>% 
#   filter(QC %in% sell_qc[sell_qc$platform_name == "PChome購物", "QC"] & 
#            early_date < as.Date(format(Sys.Date()-months(2), "%Y-%m-01"))) %>%
#   as.data.frame() 
# 
# ya_sell_date <- order %>% 
#   filter(platform_name == "Yahoo購物") %>%
#   group_by(QC) %>% 
#   summarise(early_date = min(order_day)) %>% 
#   filter(QC %in% sell_qc[sell_qc$platform_name == "Yahoo購物", "QC"] & 
#            early_date < as.Date(format(Sys.Date()-months(2), "%Y-%m-01"))) %>%
#   as.data.frame()

mo_sell_date <- order %>% 
  filter(platform_name == "momo購物") %>% 
  group_by(QC) %>% 
  summarise(early_date = min(order_day)) %>% 
  filter(QC %in% sell_qc[sell_qc$platform_name == "momo購物", "QC"] & 
           early_date <= as.Date(format(p2-months(2), "%Y-%m-01"))) %>%
  as.data.frame() 

sp_sell_date <- order %>% 
  filter(platform_name == "蝦皮購物") %>%
  group_by(QC) %>% 
  summarise(early_date = min(order_day)) %>% 
  filter(QC %in% sell_qc[sell_qc$platform_name == "蝦皮購物", "QC"] & 
           early_date <= as.Date(format(p2-months(2), "%Y-%m-01"))) %>%
  as.data.frame() 

pc_sell_date <- order %>% 
  filter(platform_name == "PChome購物") %>%
  group_by(QC) %>% 
  summarise(early_date = min(order_day)) %>% 
  filter(QC %in% sell_qc[sell_qc$platform_name == "PChome購物", "QC"] & 
           early_date < as.Date(format(p2-months(2), "%Y-%m-01"))) %>%
  as.data.frame() 

ya_sell_date <- order %>% 
  filter(platform_name == "Yahoo購物") %>%
  group_by(QC) %>% 
  summarise(early_date = min(order_day)) %>% 
  filter(QC %in% sell_qc[sell_qc$platform_name == "Yahoo購物", "QC"] & 
           early_date < as.Date(format(p2-months(2), "%Y-%m-01"))) %>%
  as.data.frame()

cat('\ncombine min order day & order data:', as.character.POSIXt(Sys.time()),'\n') # 建立從最早銷售日期至預測的區間資料+銷量資料
dates <- c()
item_id <- c()
t1 <- c()
t2 <- c()
for(i in 1:nrow(mo_sell_date)){
  t1 <- seq(from = mo_sell_date[i,2], to = p1 - 1 ,by = 1)
  dates <- c(dates, t1) 
  ndays <- (p1 - 1) - mo_sell_date[i,2] + 1
  t2 <- rep(mo_sell_date[i,1], ndays)
  item_id <- c(item_id, t2)
}
mo_data <- data.frame(timestamp = as.Date(dates), item_id = item_id) %>% 
  left_join(order %>% filter(platform_name == "momo購物") %>% group_by(order_day, QC) %>% summarise(total_sale = sum(sale_qty)), by = c("item_id" = "QC", "timestamp" = "order_day")) %>%
  mutate(total_sale = ifelse(is.na(total_sale), 0, total_sale))

dates <- c()
item_id <- c()
t1 <- c()
t2 <- c()
for(i in 1:nrow(sp_sell_date)){
  t1 <- seq(from = sp_sell_date[i,2], to = p1 - 1 ,by = 1)
  dates <- c(dates, t1) 
  ndays <- (p1 - 1) - sp_sell_date[i,2] + 1
  t2 <- rep(sp_sell_date[i,1], ndays)
  item_id <- c(item_id, t2)
}
sp_data <- data.frame(timestamp = as.Date(dates), item_id = item_id) %>% 
  left_join(order %>% filter(platform_name == "蝦皮購物") %>% group_by(order_day, QC) %>% summarise(total_sale = sum(sale_qty)), by = c("item_id" = "QC", "timestamp" = "order_day")) %>%
  mutate(total_sale = ifelse(is.na(total_sale), 0, total_sale))

dates <- c()
item_id <- c()
t1 <- c()
t2 <- c()
for(i in 1:nrow(pc_sell_date)){
  t1 <- seq(from = pc_sell_date[i,2], to = p1 - 1 ,by = 1)
  dates <- c(dates, t1) 
  ndays <- (p1 - 1) - pc_sell_date[i,2] + 1
  t2 <- rep(pc_sell_date[i,1], ndays)
  item_id <- c(item_id, t2)
}
pc_data <- data.frame(timestamp = as.Date(dates), item_id = item_id) %>% 
  left_join(order %>% filter(platform_name == "PChome購物") %>% group_by(order_day, QC) %>% summarise(total_sale = sum(sale_qty)), by = c("item_id" = "QC", "timestamp" = "order_day")) %>%
  mutate(total_sale = ifelse(is.na(total_sale), 0, total_sale))

dates <- c()
item_id <- c()
t1 <- c()
t2 <- c()
for(i in 1:nrow(ya_sell_date)){
  t1 <- seq(from = ya_sell_date[i,2], to = p1 - 1 ,by = 1)
  dates <- c(dates, t1) 
  ndays <- (p1 - 1) - ya_sell_date[i,2] + 1
  t2 <- rep(ya_sell_date[i,1], ndays)
  item_id <- c(item_id, t2)
}
ya_data <- data.frame(timestamp = as.Date(dates), item_id = item_id) %>% 
  left_join(order %>% filter(platform_name == "Yahoo購物") %>% group_by(order_day, QC) %>% summarise(total_sale = sum(sale_qty)), by = c("item_id" = "QC", "timestamp" = "order_day")) %>%
  mutate(total_sale = ifelse(is.na(total_sale), 0, total_sale))

cat('\ncombine sale activity、holiday、weekday & filetr no order qc:', as.character.POSIXt(Sys.time()),'\n') # Avene雅漾、A-Derma艾芙美為統一藥品的子品牌、匯入促銷活動、平假日、排除近一個月無銷量QC
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
  activity <- c(activity, t4)
}
act1 <- data.frame(dates = as.Date(act_date), brands = brand, display = display, activity = activity) %>% 
  filter(str_detect(brands, pattern = c("Unilever|美國康寧|Philips|Kose|Tefal|統一藥妝|Alpecin|Lock|好來|Adidas 愛迪達|Reebok|Schick 舒適牌|統一藥品")) == T & display != "團購")
brands <- data.frame(b1 = c("Unilever", "美國康寧", "Philips", "Kose", "Tefal", "統一藥妝", "Alpecin", "Lock", "好來", "Adidas 愛迪達", "Reebok", "Schick 舒適牌", "統一藥品"),
                     b2 = c("Unilever", "美國康寧", "PHILIPS 飛利浦", "KOSE-高絲", "Tefal法國特福.", "統一藥品", "沃膚", "高佳林", "Colgate", "台灣阿迪達斯", "台灣銳步", "舒適牌", "統一藥品"))
for(i in 1:nrow(brands)){
  act1[str_detect(act1$brands, pattern = brands[i,1]) == T, "brands"] <- brands[i,2]
}
display <- data.frame(d1 = c("蝦皮","Yahoo","momo","PChome"), d2 = c("蝦皮購物","Yahoo購物","momo購物","PChome購物"))
for(i in 1:nrow(display)){
  act1[str_detect(act1$display, pattern = display[i,1]) == T, "display"] <- display[i,2]
}
act1 <- act1 %>% mutate(act = 1) %>% select(dates, brands, display, act) %>% distinct()

holiday <- read.csv("C:/Users/0344_jason/Desktop/holiday.csv", header = T) %>% mutate(timestamp = as.Date(timestamp))
weekday <- read.csv("C:/Users/0344_jason/Desktop/weekday.csv", header = T) %>% mutate(timestamp = as.Date(timestamp))

# mo_no_order <- mo_data %>% 
#   group_by(item_id, year_month = paste0(year(timestamp), "-", ifelse(month(timestamp) < 10, paste0(0,month(timestamp)), month(timestamp)))) %>% 
#   summarise(total_sale = sum(total_sale)) %>% 
#   filter(total_sale == 0 & year_month == paste0(year(Sys.Date() - day(Sys.Date())), "-", ifelse(month(Sys.Date() - day(Sys.Date())) < 10, paste0(0, month(Sys.Date() - day(Sys.Date()))), month(Sys.Date() - day(Sys.Date())))))

mo_no_order <- mo_data %>% 
  group_by(item_id, year_month = paste0(year(timestamp), "-", ifelse(month(timestamp) < 10, paste0(0,month(timestamp)), month(timestamp)))) %>% 
  summarise(total_sale = sum(total_sale)) %>% 
  filter(total_sale == 0 & year_month == paste0(year(p2 - day(p2)), "-", ifelse(month(p2 - day(p2)) < 10, paste0(0, month(p2 - day(p2))), month(p2 - day(p2)))))

mo_train_data <- mo_data %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>% 
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品",
                                  parent_brand == "Avene雅漾" ~ "統一藥品",
                                  T ~ parent_brand)) %>%
  left_join(act1 %>% filter(display == "momo購物"), by = c("parent_brand" = "brands", "timestamp" = "dates")) %>%
  mutate(display = "momo購物", act = ifelse(is.na(act), 0, act)) %>% 
  left_join(holiday, by = "timestamp")  %>% 
  left_join(weekday, by = "timestamp") %>% 
  mutate(week = weekdays(timestamp), holiday = if_else(is.na(holiday) == 0, 1, 0), 
         weekday = if_else(is.na(weekday) == 0, 1, 0), week = if_else(week %in% c("星期六","星期日"), 1, 0),
         com = paste(holiday, weekday, week), com1 = if_else(com %in% c("0 1 1","0 0 0"), 0, 1)) %>% 
  select(timestamp, item_id, total_sale, act, com1) %>% rename("day_type" = "com1") %>% 
  filter(!item_id %in% unique(mo_no_order$item_id)) %>%
  as.data.frame()

# sp_no_order <- sp_data %>% 
#   group_by(item_id, year_month = paste0(year(timestamp), "-", ifelse(month(timestamp) < 10, paste0(0,month(timestamp)), month(timestamp)))) %>% 
#   summarise(total_sale = sum(total_sale)) %>% 
#   filter(total_sale == 0 & year_month == paste0(year(Sys.Date() - day(Sys.Date())), "-", ifelse(month(Sys.Date() - day(Sys.Date())) < 10, paste0(0, month(Sys.Date() - day(Sys.Date()))), month(Sys.Date() - day(Sys.Date())))))

sp_no_order <- sp_data %>% 
  group_by(item_id, year_month = paste0(year(timestamp), "-", ifelse(month(timestamp) < 10, paste0(0,month(timestamp)), month(timestamp)))) %>% 
  summarise(total_sale = sum(total_sale)) %>% 
  filter(total_sale == 0 & year_month == paste0(year(p2 - day(p2)), "-", ifelse(month(p2 - day(p2)) < 10, paste0(0, month(p2 - day(p2))), month(p2 - day(p2)))))


sp_train_data <- sp_data %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>% 
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品",
                                  parent_brand == "Avene雅漾" ~ "統一藥品",
                                  T ~ parent_brand)) %>%
  left_join(act1 %>% filter(display == "蝦皮購物"), by = c("parent_brand" = "brands", "timestamp" = "dates")) %>%
  mutate(display = "蝦皮購物", act = ifelse(is.na(act), 0, act)) %>% 
  left_join(holiday, by = "timestamp")  %>% 
  left_join(weekday, by = "timestamp") %>% 
  mutate(week = weekdays(timestamp), holiday = if_else(is.na(holiday) == 0, 1, 0), 
         weekday = if_else(is.na(weekday) == 0, 1, 0), week = if_else(week %in% c("星期六","星期日"), 1, 0),
         com = paste(holiday, weekday, week), com1 = if_else(com %in% c("0 1 1","0 0 0"), 0, 1)) %>% 
  select(timestamp, item_id, total_sale, act, com1) %>% rename("day_type" = "com1") %>% 
  filter(!item_id %in% unique(sp_no_order$item_id)) %>%
  as.data.frame()


# pc_no_order <- pc_data %>% 
#   group_by(item_id, year_month = paste0(year(timestamp), "-", ifelse(month(timestamp) < 10, paste0(0,month(timestamp)), month(timestamp)))) %>% 
#   summarise(total_sale = sum(total_sale)) %>% 
#   filter(total_sale == 0 & year_month == paste0(year(Sys.Date() - day(Sys.Date())), "-", ifelse(month(Sys.Date() - day(Sys.Date())) < 10, paste0(0, month(Sys.Date() - day(Sys.Date()))), month(Sys.Date() - day(Sys.Date())))))

pc_no_order <- pc_data %>% 
  group_by(item_id, year_month = paste0(year(timestamp), "-", ifelse(month(timestamp) < 10, paste0(0,month(timestamp)), month(timestamp)))) %>% 
  summarise(total_sale = sum(total_sale)) %>% 
  filter(total_sale == 0 & year_month == paste0(year(p2 - day(p2)), "-", ifelse(month(p2 - day(p2)) < 10, paste0(0, month(p2 - day(p2))), month(p2 - day(p2)))))

pc_train_data <- pc_data %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>% 
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品",
                                  parent_brand == "Avene雅漾" ~ "統一藥品",
                                  T ~ parent_brand)) %>%
  left_join(act1 %>% filter(display == "PChome購物"), by = c("parent_brand" = "brands", "timestamp" = "dates")) %>%
  mutate(display = "PChome購物", act = ifelse(is.na(act), 0, act)) %>% 
  left_join(holiday, by = "timestamp")  %>% 
  left_join(weekday, by = "timestamp") %>% 
  mutate(week = weekdays(timestamp), holiday = if_else(is.na(holiday) == 0, 1, 0), 
         weekday = if_else(is.na(weekday) == 0, 1, 0), week = if_else(week %in% c("星期六","星期日"), 1, 0),
         com = paste(holiday, weekday, week), com1 = if_else(com %in% c("0 1 1","0 0 0"), 0, 1)) %>% 
  select(timestamp, item_id, total_sale, act, com1) %>% rename("day_type" = "com1") %>% 
  filter(!item_id %in% unique(pc_no_order$item_id)) %>%
  as.data.frame()

# ya_no_order <- ya_data %>% 
#   group_by(item_id, year_month = paste0(year(timestamp), "-", ifelse(month(timestamp) < 10, paste0(0,month(timestamp)), month(timestamp)))) %>% 
#   summarise(total_sale = sum(total_sale)) %>% 
#   filter(total_sale == 0 & year_month == paste0(year(Sys.Date() - day(Sys.Date())), "-", ifelse(month(Sys.Date() - day(Sys.Date())) < 10, paste0(0, month(Sys.Date() - day(Sys.Date()))), month(Sys.Date() - day(Sys.Date())))))

ya_no_order <- ya_data %>% 
  group_by(item_id, year_month = paste0(year(timestamp), "-", ifelse(month(timestamp) < 10, paste0(0,month(timestamp)), month(timestamp)))) %>% 
  summarise(total_sale = sum(total_sale)) %>% 
  filter(total_sale == 0 & year_month == paste0(year(p2 - day(p2)), "-", ifelse(month(p2 - day(p2)) < 10, paste0(0, month(p2 - day(p2))), month(p2 - day(p2)))))

ya_train_data <- ya_data %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>% 
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品",
                                  parent_brand == "Avene雅漾" ~ "統一藥品",
                                  T ~ parent_brand)) %>%
  left_join(act1 %>% filter(display == "Yahoo購物"), by = c("parent_brand" = "brands", "timestamp" = "dates")) %>%
  mutate(display = "Yahoo購物", act = ifelse(is.na(act), 0, act)) %>% 
  left_join(holiday, by = "timestamp")  %>% 
  left_join(weekday, by = "timestamp") %>% 
  mutate(week = weekdays(timestamp), holiday = if_else(is.na(holiday) == 0, 1, 0), 
         weekday = if_else(is.na(weekday) == 0, 1, 0), week = if_else(week %in% c("星期六","星期日"), 1, 0),
         com = paste(holiday, weekday, week), com1 = if_else(com %in% c("0 1 1","0 0 0"), 0, 1)) %>% 
  select(timestamp, item_id, total_sale, act, com1) %>% rename("day_type" = "com1") %>% 
  filter(!item_id %in% unique(ya_no_order$item_id)) %>%
  as.data.frame()

mo_train_data$order <- 1:nrow(mo_train_data)
sp_train_data$order <- 1:nrow(sp_train_data)
pc_train_data$order <- 1:nrow(pc_train_data)
ya_train_data$order <- 1:nrow(ya_train_data)

cat('\nmomo arima:', as.character.POSIXt(Sys.time()),'\n')
# arima模型建立
# test = "adf"使用ADF測試來確定差分次數，使時序數據更平穩，但須具有顯著性(P值<0.05)
# 若不指定 test 參數，則使用 AIC進行模型選擇，再依時序數據是否平穩下搜尋適合的模型
t1 <- c()
t2 <- c()
t3 <- unique(mo_train_data$item_id)
item_id1 <- c()
item_id2 <- c()
fore_arima1 <- c()
fore_arima2 <- c()
for(i in 1:length(t3)){
  a1 <- as.matrix(mo_train_data[, c("act", "day_type")]) 
  x1 <- ts(mo_train_data[mo_train_data$item_id == t3[i] & mo_train_data$timestamp < p2, "total_sale"], frequency = 7)
  a2 <- mo_train_data[mo_train_data$item_id == t3[i] & mo_train_data$timestamp < p2, "order"]
  x2 <- 1:length(a2)
  if(sum(a1[a2,1]) > 0){
    if(adf.test(x1[x2])$p.value < 0.05){
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], test = "adf", stepwise = F, trace = F, ic = c("aic"))
      a3 <- mo_train_data[mo_train_data$item_id == t3[i] & mo_train_data$timestamp > (p2-1) & mo_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id1 <- c(item_id1, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima1 <- c(fore_arima1, t2)
    }else if(is.null(tryCatch(
      auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic")),
      error = function(e) {
        NULL
      })) == F){
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic"))
      a3 <- mo_train_data[mo_train_data$item_id == t3[i] & mo_train_data$timestamp > (p2-1) & mo_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id1 <- c(item_id1, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima1 <- c(fore_arima1, t2)
    }else{
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = F, ic = c("aic"))
      a3 <- mo_train_data[mo_train_data$item_id == t3[i] & mo_train_data$timestamp > (p2-1) & mo_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id1 <- c(item_id1, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima1 <- c(fore_arima1, t2)
    }
  }else{
    if(adf.test(x1[x2])$p.value < 0.05){
      a1 <- as.matrix(mo_train_data[, "day_type"])
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], test = "adf", stepwise = F, trace = F,  ic = c("aic"))
      a3 <- mo_train_data[mo_train_data$item_id == t3[i] & mo_train_data$timestamp > (p2-1) & mo_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id2 <- c(item_id2, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima2 <- c(fore_arima2, t2)
    }else if(is.null(tryCatch(
      auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic")),
      error = function(e) {
        NULL
      })) == F){
      a1 <- as.matrix(mo_train_data[, "day_type"])
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic"))
      a3 <- mo_train_data[mo_train_data$item_id == t3[i] & mo_train_data$timestamp > (p2-1) & mo_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id2 <- c(item_id2, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima2 <- c(fore_arima2, t2)
    }else{
      a1 <- as.matrix(mo_train_data[, "day_type"])
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = F, ic = c("aic"))
      a3 <- mo_train_data[mo_train_data$item_id == t3[i] & mo_train_data$timestamp > (p2-1) & mo_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id2 <- c(item_id2, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima2 <- c(fore_arima2, t2)
    }
  }
}

pred_arima_mo <- data.frame(item_id = c(item_id1, item_id2), pred_value = c(fore_arima1, fore_arima2)) %>%
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         pred_value = round(ifelse(is.na(pred_value) == T|pred_value < 0, 0, pred_value), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "momo購物", 
         model = "arima") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(pred_value)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

cat('\nshopee arima:', as.character.POSIXt(Sys.time()),'\n')
t1 <- c()
t2 <- c()
t3 <- unique(sp_train_data$item_id)
item_id1 <- c()
item_id2 <- c()
fore_arima1 <- c()
fore_arima2 <- c()
for(i in 1:length(t3)){
  a1 <- as.matrix(sp_train_data[, c("act", "day_type")]) # act day_type
  x1 <- ts(sp_train_data[sp_train_data$item_id == t3[i] & sp_train_data$timestamp < p2, "total_sale"], frequency = 7)
  a2 <- sp_train_data[sp_train_data$item_id == t3[i] & sp_train_data$timestamp < p2, "order"]
  x2 <- 1:length(a2)
  if(sum(a1[a2,1]) > 0){
    if(adf.test(x1[x2])$p.value < 0.05){
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], test = "adf", stepwise = F, trace = F, ic = c("aic"))
      a3 <- sp_train_data[sp_train_data$item_id == t3[i] & sp_train_data$timestamp > (p2-1) & sp_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id1 <- c(item_id1, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima1 <- c(fore_arima1, t2)
    }else if(is.null(tryCatch(
      auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic")),
      error = function(e) {
        NULL
      })) == F){
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, stationary = T, trace = F, ic = c("aic"))
      a3 <- sp_train_data[sp_train_data$item_id == t3[i] & sp_train_data$timestamp > (p2-1) & sp_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id1 <- c(item_id1, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima1 <- c(fore_arima1, t2)
    }else{
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = F, ic = c("aic"))
      a3 <- sp_train_data[sp_train_data$item_id == t3[i] & sp_train_data$timestamp > (p2-1) & sp_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id1 <- c(item_id1, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima1 <- c(fore_arima1, t2)
    }
  }else{
    if(adf.test(x1[x2])$p.value < 0.05){
      a1 <- as.matrix(sp_train_data[, "day_type"])
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], test = "adf", stepwise = F, trace = F, ic = c("aic"))
      a3 <- sp_train_data[sp_train_data$item_id == t3[i] & sp_train_data$timestamp > (p2-1) & sp_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id2 <- c(item_id2, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima2 <- c(fore_arima2, t2)
    }else if(is.null(tryCatch(
      auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic")),
      error = function(e) {
        NULL
      })) == F){
      a1 <- as.matrix(sp_train_data[, "day_type"])
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic"))
      a3 <- sp_train_data[sp_train_data$item_id == t3[i] & sp_train_data$timestamp > (p2-1) & sp_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id2 <- c(item_id2, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima2 <- c(fore_arima2, t2)
    }else{
      a1 <- as.matrix(sp_train_data[, "day_type"])
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = F, ic = c("aic"))
      a3 <- sp_train_data[sp_train_data$item_id == t3[i] & sp_train_data$timestamp > (p2-1) & sp_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id2 <- c(item_id2, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima2 <- c(fore_arima2, t2)
    }
  }
}

pred_arima_sp <- data.frame(item_id = c(item_id1, item_id2), pred_value = c(fore_arima1, fore_arima2)) %>%
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         pred_value = round(ifelse(is.na(pred_value) == T|pred_value < 0, 0, pred_value), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "蝦皮購物", 
         model = "arima") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(pred_value)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

cat('\npchome arima:', as.character.POSIXt(Sys.time()),'\n')
t1 <- c()
t2 <- c()
t3 <- unique(pc_train_data$item_id)
item_id1 <- c()
item_id2 <- c()
fore_arima1 <- c()
fore_arima2 <- c()
for(i in 1:length(t3)){
  a1 <- as.matrix(pc_train_data[, c("act", "day_type")]) # act day_type
  x1 <- ts(pc_train_data[pc_train_data$item_id == t3[i] & pc_train_data$timestamp < p2, "total_sale"], frequency = 7)
  a2 <- pc_train_data[pc_train_data$item_id == t3[i] & pc_train_data$timestamp < p2, "order"]
  x2 <- 1:length(a2)
  if(sum(a1[a2,1]) > 0){
    if(adf.test(x1[x2])$p.value < 0.05){
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], test = "adf", stepwise = F, trace = F, ic = c("aic"))
      a3 <- pc_train_data[pc_train_data$item_id == t3[i] & pc_train_data$timestamp > (p2-1) & pc_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id1 <- c(item_id1, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima1 <- c(fore_arima1, t2)
    }else if(is.null(tryCatch(
      auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic")),
      error = function(e) {
        NULL
      })) == F){
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic"))
      a3 <- pc_train_data[pc_train_data$item_id == t3[i] & pc_train_data$timestamp > (p2-1) & pc_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id1 <- c(item_id1, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima1 <- c(fore_arima1, t2)
    }else{
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = F, ic = c("aic"))
      a3 <- pc_train_data[pc_train_data$item_id == t3[i] & pc_train_data$timestamp > (p2-1) & pc_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id1 <- c(item_id1, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima1 <- c(fore_arima1, t2)
    }
  }else{
    if(adf.test(x1[x2])$p.value < 0.05){
      a1 <- as.matrix(sp_train_data[, "day_type"])
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], test = "adf", stepwise = F, trace = F, ic = c("aic"))
      a3 <- pc_train_data[pc_train_data$item_id == t3[i] & pc_train_data$timestamp > (p2-1) & pc_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id2 <- c(item_id2, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima2 <- c(fore_arima2, t2)
    }else if(is.null(tryCatch(
      auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic")),
      error = function(e) {
        NULL
      })) == F){
      a1 <- as.matrix(sp_train_data[, "day_type"])
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic"))
      a3 <- pc_train_data[pc_train_data$item_id == t3[i] & pc_train_data$timestamp > (p2-1) & pc_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id2 <- c(item_id2, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima2 <- c(fore_arima2, t2)
    }else{
      a1 <- as.matrix(sp_train_data[, "day_type"])
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = F, ic = c("aic"))
      a3 <- pc_train_data[pc_train_data$item_id == t3[i] & pc_train_data$timestamp > (p2-1) & pc_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id2 <- c(item_id2, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima2 <- c(fore_arima2, t2)
    }
  }
}

pred_arima_pc <- data.frame(item_id = c(item_id1, item_id2), pred_value = c(fore_arima1, fore_arima2)) %>%
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         pred_value = round(ifelse(is.na(pred_value) == T|pred_value < 0, 0, pred_value), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "PChome購物", 
         model = "arima") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(pred_value)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

cat('\nyahoo arima:', as.character.POSIXt(Sys.time()),'\n')
t1 <- c()
t2 <- c()
t3 <- unique(ya_train_data$item_id)
item_id1 <- c()
item_id2 <- c()
fore_arima1 <- c()
fore_arima2 <- c()
for(i in 1:length(t3)){
  a1 <- as.matrix(ya_train_data[, c("act", "day_type")]) # act day_type
  x1 <- ts(ya_train_data[ya_train_data$item_id == t3[i] & ya_train_data$timestamp < p2, "total_sale"], frequency = 7)
  a2 <- ya_train_data[ya_train_data$item_id == t3[i] & ya_train_data$timestamp < p2, "order"]
  x2 <- 1:length(a2)
  if(sum(a1[a2,1]) > 0){
    if(adf.test(x1[x2])$p.value < 0.05){
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], test = "adf", stepwise = F, trace = F, ic = c("aic"))
      a3 <- ya_train_data[ya_train_data$item_id == t3[i] & ya_train_data$timestamp > (p2-1) & ya_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id1 <- c(item_id1, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima1 <- c(fore_arima1, t2)
    }else if(is.null(tryCatch(
      auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic")),
      error = function(e) {
        NULL
      })) == F){
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic"))
      a3 <- ya_train_data[ya_train_data$item_id == t3[i] & ya_train_data$timestamp > (p2-1) & ya_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id1 <- c(item_id1, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima1 <- c(fore_arima1, t2)
    }else{
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = F, ic = c("aic"))
      a3 <- ya_train_data[sp_train_data$item_id == t3[i] & ya_train_data$timestamp > (p2-1) & ya_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id1 <- c(item_id1, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima1 <- c(fore_arima1, t2)
    }
  }else{
    if(adf.test(x1[x2])$p.value < 0.05){
      a1 <- as.matrix(ya_train_data[, "day_type"])
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], test = "adf", stepwise = F, trace = F, ic = c("aic"))
      a3 <- ya_train_data[ya_train_data$item_id == t3[i] & ya_train_data$timestamp > (p2-1) & ya_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id2 <- c(item_id2, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima2 <- c(fore_arima2, t2)
    }else if(is.null(tryCatch(
      auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic")),
      error = function(e) {
        NULL
      })) == F){
      a1 <- as.matrix(ya_train_data[, "day_type"])
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = T, ic = c("aic"))
      a3 <- ya_train_data[ya_train_data$item_id == t3[i] & ya_train_data$timestamp > (p2-1) & ya_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id2 <- c(item_id2, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima2 <- c(fore_arima2, t2)
    }else{
      a1 <- as.matrix(ya_train_data[, "day_type"])
      undis_model <- auto.arima(x1[x2], xreg = a1[a2,], stepwise = F, trace = F, stationary = F, ic = c("aic"))
      a3 <- ya_train_data[ya_train_data$item_id == t3[i] & ya_train_data$timestamp > (p2-1) & ya_train_data$timestamp < p1, "order"]
      t1 <- rep(t3[i], length(a3)) #item_id
      item_id2 <- c(item_id2, t1)
      t2 <- forecast(undis_model, xreg = a1[a3,])[["mean"]][1:length(a3)] #predict
      fore_arima2 <- c(fore_arima2, t2)
    }
  }
}

pred_arima_ya <- data.frame(item_id = c(item_id1, item_id2), pred_value = c(fore_arima1, fore_arima2)) %>%
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         pred_value = round(ifelse(is.na(pred_value) == T|pred_value < 0, 0, pred_value), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "Yahoo購物", 
         model = "arima") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(pred_value)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_arima <- rbind(pred_arima_mo, pred_arima_sp, pred_arima_pc, pred_arima_ya)


# ets
cat('\nmomo ets:', as.character.POSIXt(Sys.time()),'\n')
item_id <- c()
ets_ann <- c()
ets_aaa <- c()
ets_ana <- c()
ets_aan <- c()
ets_zzz <- c()
t1 <- c()
t2 <- c()
t3 <- c()
t4 <- c()
t5 <- c()
t6 <- c()
t7 <- unique(mo_train_data$item_id)
for(i in 1:length(t7)){
  q1 <- ets(ts(mo_train_data[mo_train_data$item_id == t7[i] & mo_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "ANN")
  q2 <- ets(ts(mo_train_data[mo_train_data$item_id == t7[i] & mo_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "AAA")
  q3 <- ets(ts(mo_train_data[mo_train_data$item_id == t7[i] & mo_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "ANA")
  q4 <- ets(ts(mo_train_data[mo_train_data$item_id == t7[i] & mo_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "AAN")
  q5 <- ets(ts(mo_train_data[mo_train_data$item_id == t7[i] & mo_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "ZZZ")
  t1 <- forecast(q1, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_ann <- c(ets_ann, t1)
  t2 <- forecast(q2, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_aaa <- c(ets_aaa, t2)
  t3 <- forecast(q3, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_ana <- c(ets_ana, t3)
  t4 <- forecast(q4, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_aan <- c(ets_aan, t4)
  t5 <- forecast(q5, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_zzz <- c(ets_zzz, t5)
  t6 <- rep(t7[i], as.numeric(p1-p2))
  item_id <- c(item_id, t6)
}
pred_ets_ann_mo <- data.frame(item_id = item_id, ets_ann = ets_ann) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_ann = round(ifelse(is.na(ets_ann) == T|ets_ann < 0, 0, ets_ann), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "momo購物", 
         model = "ets_ann") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_ann)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_aaa_mo <- data.frame(item_id = item_id, ets_aaa = ets_aaa) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_aaa = round(ifelse(is.na(ets_aaa) == T|ets_aaa < 0, 0, ets_aaa), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "momo購物", 
         model = "ets_aaa") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_aaa)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_ana_mo <- data.frame(item_id = item_id, ets_ana = ets_ana) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_ana = round(ifelse(is.na(ets_ana) == T|ets_ana < 0, 0, ets_ana), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "momo購物", 
         model = "ets_ana") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_ana)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_aan_mo <- data.frame(item_id = item_id, ets_aan = ets_aan) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_aan = round(ifelse(is.na(ets_aan) == T|ets_aan < 0, 0, ets_aan), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "momo購物", 
         model = "ets_aan") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_aan)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_zzz_mo <- data.frame(item_id = item_id, ets_zzz = ets_zzz) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_aan = round(ifelse(is.na(ets_aan) == T|ets_aan < 0, 0, ets_aan), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "momo購物", 
         model = "ets_zzz") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_zzz)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

cat('\nshopee ets:', as.character.POSIXt(Sys.time()),'\n')
item_id <- c()
ets_ann <- c()
ets_aaa <- c()
ets_ana <- c()
ets_aan <- c()
ets_zzz <- c()
t1 <- c()
t2 <- c()
t3 <- c()
t4 <- c()
t5 <- c()
t6 <- c()
t7 <- unique(sp_train_data$item_id)
for(i in 1:length(t7)){
  q1 <- ets(ts(sp_train_data[sp_train_data$item_id == t7[i] & sp_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "ANN")
  q2 <- ets(ts(sp_train_data[sp_train_data$item_id == t7[i] & sp_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "AAA")
  q3 <- ets(ts(sp_train_data[sp_train_data$item_id == t7[i] & sp_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "ANA")
  q4 <- ets(ts(sp_train_data[sp_train_data$item_id == t7[i] & sp_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "AAN")
  q5 <- ets(ts(sp_train_data[sp_train_data$item_id == t7[i] & sp_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "ZZZ")
  t1 <- forecast(q1, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_ann <- c(ets_ann, t1)
  t2 <- forecast(q2, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_aaa <- c(ets_aaa, t2)
  t3 <- forecast(q3, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_ana <- c(ets_ana, t3)
  t4 <- forecast(q4, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_aan <- c(ets_aan, t4)
  t5 <- forecast(q5, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_zzz <- c(ets_zzz, t5)
  t6 <- rep(t7[i], as.numeric(p1-p2))
  item_id <- c(item_id, t6)
}
pred_ets_ann_sp <- data.frame(item_id = item_id, ets_ann = ets_ann) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_ann = round(ifelse(is.na(ets_ann) == T|ets_ann < 0, 0, ets_ann), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "蝦皮購物", 
         model = "ets_ann") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_ann)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_aaa_sp <- data.frame(item_id = item_id, ets_aaa = ets_aaa) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_aaa = round(ifelse(is.na(ets_aaa) == T|ets_aaa < 0, 0, ets_aaa), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "蝦皮購物", 
         model = "ets_aaa") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_aaa)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_ana_sp <- data.frame(item_id = item_id, ets_ana = ets_ana) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_ana = round(ifelse(is.na(ets_ana) == T|ets_ana < 0, 0, ets_ana), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "蝦皮購物", 
         model = "ets_ana") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_ana)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_aan_sp <- data.frame(item_id = item_id, ets_aan = ets_aan) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_aan = round(ifelse(is.na(ets_aan) == T|ets_aan < 0, 0, ets_aan), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "蝦皮購物", 
         model = "ets_aan") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_aan)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_zzz_sp <- data.frame(item_id = item_id, ets_zzz = ets_zzz) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_zzz = round(ifelse(is.na(ets_zzz) == T|ets_zzz < 0, 0, ets_zzz), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "蝦皮購物", 
         model = "ets_zzz") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_zzz)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

cat('\npchome ets:', as.character.POSIXt(Sys.time()),'\n')
item_id <- c()
ets_ann <- c()
ets_aaa <- c()
ets_ana <- c()
ets_aan <- c()
ets_zzz <- c()
t1 <- c()
t2 <- c()
t3 <- c()
t4 <- c()
t5 <- c()
t6 <- c()
t7 <- unique(pc_train_data$item_id)
for(i in 1:length(t7)){
  q1 <- ets(ts(pc_train_data[pc_train_data$item_id == t7[i] & pc_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "ANN")
  q2 <- ets(ts(pc_train_data[pc_train_data$item_id == t7[i] & pc_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "AAA")
  q3 <- ets(ts(pc_train_data[pc_train_data$item_id == t7[i] & pc_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "ANA")
  q4 <- ets(ts(pc_train_data[pc_train_data$item_id == t7[i] & pc_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "AAN")
  q5 <- ets(ts(pc_train_data[pc_train_data$item_id == t7[i] & pc_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "ZZZ")
  t1 <- forecast(q1, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_ann <- c(ets_ann, t1)
  t2 <- forecast(q2, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_aaa <- c(ets_aaa, t2)
  t3 <- forecast(q3, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_ana <- c(ets_ana, t3)
  t4 <- forecast(q4, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_aan <- c(ets_aan, t4)
  t5 <- forecast(q5, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_zzz <- c(ets_zzz, t5)
  t6 <- rep(t7[i], as.numeric(p1-p2))
  item_id <- c(item_id, t6)
}
pred_ets_ann_pc <- data.frame(item_id = item_id, ets_ann = ets_ann) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_ann = round(ifelse(is.na(ets_ann) == T|ets_ann < 0, 0, ets_ann), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "PChome購物", 
         model = "ets_ann") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_ann)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_aaa_pc <- data.frame(item_id = item_id, ets_aaa = ets_aaa) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_aaa = round(ifelse(is.na(ets_aaa) == T|ets_aaa < 0, 0, ets_aaa), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "PChome購物", 
         model = "ets_aaa") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_aaa)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_ana_pc <- data.frame(item_id = item_id, ets_ana = ets_ana) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_ana = round(ifelse(is.na(ets_ana) == T|ets_ana < 0, 0, ets_ana), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "PChome購物", 
         model = "ets_ana") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_ana)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_aan_pc <- data.frame(item_id = item_id, ets_aan = ets_aan) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_aan = round(ifelse(is.na(ets_aan) == T|ets_aan < 0, 0, ets_aan), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "PChome購物", 
         model = "ets_aan") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_aan)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_zzz_pc <- data.frame(item_id = item_id, ets_zzz = ets_zzz) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_zzz = round(ifelse(is.na(ets_zzz) == T|ets_zzz < 0, 0, ets_zzz), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "PChome購物", 
         model = "ets_zzz") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_zzz)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

cat('\nyahoo ets:', as.character.POSIXt(Sys.time()),'\n')
item_id <- c()
ets_ann <- c()
ets_aaa <- c()
ets_ana <- c()
ets_aan <- c()
ets_zzz <- c()
t1 <- c()
t2 <- c()
t3 <- c()
t4 <- c()
t5 <- c()
t6 <- c()
t7 <- unique(ya_train_data$item_id)
for(i in 1:length(t7)){
  q1 <- ets(ts(ya_train_data[ya_train_data$item_id == t7[i] & ya_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "ANN")
  q2 <- ets(ts(ya_train_data[ya_train_data$item_id == t7[i] & ya_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "AAA")
  q3 <- ets(ts(ya_train_data[ya_train_data$item_id == t7[i] & ya_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "ANA")
  q4 <- ets(ts(ya_train_data[ya_train_data$item_id == t7[i] & ya_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "AAN")
  q5 <- ets(ts(ya_train_data[ya_train_data$item_id == t7[i] & ya_train_data$timestamp < p2, "total_sale"], frequency = 7), model = "ZZZ")
  t1 <- forecast(q1, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_ann <- c(ets_ann, t1)
  t2 <- forecast(q2, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_aaa <- c(ets_aaa, t2)
  t3 <- forecast(q3, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_ana <- c(ets_ana, t3)
  t4 <- forecast(q4, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_aan <- c(ets_aan, t4)
  t5 <- forecast(q5, as.numeric(p1-p2))[["mean"]][1:as.numeric(p1-p2)]
  ets_zzz <- c(ets_zzz, t5)
  t6 <- rep(t7[i], as.numeric(p1-p2))
  item_id <- c(item_id, t6)
}
pred_ets_ann_ya <- data.frame(item_id = item_id, ets_ann = ets_ann) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_ann = round(ifelse(is.na(ets_ann) == T|ets_ann < 0, 0, ets_ann), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "Yahoo購物", 
         model = "ets_ann") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_ann)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_aaa_ya <- data.frame(item_id = item_id, ets_aaa = ets_aaa) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_aaa = round(ifelse(is.na(ets_aaa) == T|ets_aaa < 0, 0, ets_aaa), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "Yahoo購物", 
         model = "ets_aaa") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_aaa)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_ana_ya <- data.frame(item_id = item_id, ets_ana = ets_ana) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_ana = round(ifelse(is.na(ets_ana) == T|ets_ana < 0, 0, ets_ana), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "Yahoo購物", 
         model = "ets_ana") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_ana)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_aan_ya <- data.frame(item_id = item_id, ets_aan = ets_aan) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_aan = round(ifelse(is.na(ets_aan) == T|ets_aan < 0, 0, ets_aan), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "Yahoo購物", 
         model = "ets_aan") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_aan)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets_zzz_ya <- data.frame(item_id = item_id, ets_zzz = ets_zzz) %>% 
  mutate(date = rep(seq.Date(from = as.Date(p2,format = "%Y-%m-%d"), by = "day", length.out = as.numeric(p1-p2)), length(unique(item_id))), 
         ets_zzz = round(ifelse(is.na(ets_zzz) == T|ets_zzz < 0, 0, ets_zzz), 0)) %>% 
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         platform_name = "Yahoo購物", 
         model = "ets_zzz") %>%
  group_by(item_id, 
           year_month = paste0(year(date), "-", ifelse(month(date) < 10, paste0(0, month(date)), month(date))),
           parent_brand, 
           platform_name,
           model) %>%
  summarise(pred_value = sum(ets_zzz)) %>%
  filter(year_month == paste0(year(p1-day(p1)), "-", ifelse(month(p1-day(p1)) < 10, paste0(0, month(p1-day(p1))), month(p1-day(p1)))))

pred_ets <-rbind(pred_ets_ann_mo, pred_ets_aaa_mo, pred_ets_ana_mo, pred_ets_aan_mo, pred_ets_zzz_mo, 
                 pred_ets_ann_sp, pred_ets_aaa_sp, pred_ets_ana_sp, pred_ets_aan_sp, pred_ets_zzz_sp,
                 pred_ets_ann_pc, pred_ets_aaa_pc, pred_ets_ana_pc, pred_ets_aan_pc, pred_ets_zzz_pc,
                 pred_ets_ann_ya, pred_ets_aaa_ya, pred_ets_ana_ya, pred_ets_aan_ya, pred_ets_zzz_ya)
# 近3期
mo_thr <- mo_train_data %>% 
  filter(timestamp >= p3 & timestamp < p7) %>% 
  group_by(item_id) %>% 
  summarise(pred_value = round(sum(total_sale)/3, 0)) %>%
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         year_month = paste0(year(p2 %m+% months(1)), "-", ifelse(month(p2 %m+% months(1)) < 10, paste0(0, month(p2 %m+% months(1))), month(p2 %m+% months(1)))),
         model = "thr_mon", platform_name = "momo購物")

sp_thr <- sp_train_data %>% 
  filter(timestamp >= p3 & timestamp < p7) %>% 
  group_by(item_id) %>% 
  summarise(pred_value = round(sum(total_sale)/3, 0)) %>%
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         year_month = paste0(year(p2 %m+% months(1)), "-", ifelse(month(p2 %m+% months(1)) < 10, paste0(0, month(p2 %m+% months(1))), month(p2 %m+% months(1)))),
         model = "thr_mon", platform_name = "蝦皮購物")

pc_thr <- pc_train_data %>% 
  filter(timestamp >= p3 & timestamp < p7) %>% 
  group_by(item_id) %>% 
  summarise(pred_value = round(sum(total_sale)/3, 0)) %>%
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         year_month = paste0(year(p2 %m+% months(1)), "-", ifelse(month(p2 %m+% months(1)) < 10, paste0(0, month(p2 %m+% months(1))), month(p2 %m+% months(1)))),
         model = "thr_mon", platform_name = "PChome購物")

ya_thr <- ya_train_data %>% 
  filter(timestamp >= p3 & timestamp < p7) %>% 
  group_by(item_id) %>% 
  summarise(pred_value = round(sum(total_sale)/3, 0)) %>%
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         year_month = paste0(year(p2 %m+% months(1)), "-", ifelse(month(p2 %m+% months(1)) < 10, paste0(0, month(p2 %m+% months(1))), month(p2 %m+% months(1)))),
         model = "thr_mon", platform_name = "Yahoo購物")

# 近6期
mo_six <- mo_train_data %>% 
  filter(timestamp >= p4 & timestamp < p7) %>% 
  group_by(item_id) %>% 
  summarise(pred_value = round(sum(total_sale)/6, 0)) %>%
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         year_month = paste0(year(p2 %m+% months(1)), "-", ifelse(month(p2 %m+% months(1)) < 10, paste0(0, month(p2 %m+% months(1))), month(p2 %m+% months(1)))),
         model = "six_mon", platform_name = "momo購物")
sp_six <- sp_train_data %>% 
  filter(timestamp >= p4 & timestamp < p7) %>% 
  group_by(item_id) %>% 
  summarise(pred_value = round(sum(total_sale)/6, 0)) %>%
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         year_month = paste0(year(p2 %m+% months(1)), "-", ifelse(month(p2 %m+% months(1)) < 10, paste0(0, month(p2 %m+% months(1))), month(p2 %m+% months(1)))),
         model = "six_mon", platform_name = "蝦皮購物")
pc_six <- pc_train_data %>% 
  filter(timestamp >= p4 & timestamp < p7) %>% 
  group_by(item_id) %>% 
  summarise(pred_value = round(sum(total_sale)/6, 0)) %>%
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         year_month = paste0(year(p2 %m+% months(1)), "-", ifelse(month(p2 %m+% months(1)) < 10, paste0(0, month(p2 %m+% months(1))), month(p2 %m+% months(1)))),
         model = "six_mon", platform_name = "PChome購物")
ya_six <- ya_train_data %>% 
  filter(timestamp >= p4 & timestamp < p7) %>% 
  group_by(item_id) %>% 
  summarise(pred_value = round(sum(total_sale)/6, 0)) %>%
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         year_month = paste0(year(p2 %m+% months(1)), "-", ifelse(month(p2 %m+% months(1)) < 10, paste0(0, month(p2 %m+% months(1))), month(p2 %m+% months(1)))),
         model = "six_mon", platform_name = "Yahoo購物")

# 同期
mo_sam <- mo_train_data %>% 
  filter(timestamp >= p5 & timestamp < p6) %>% 
  group_by(item_id) %>% 
  summarise(pred_value = sum(total_sale)) %>%
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         year_month = paste0(year(p2 %m+% months(1)), "-", ifelse(month(p2 %m+% months(1)) < 10, paste0(0, month(p2 %m+% months(1))), month(p2 %m+% months(1)))),
         model = "sam_mon", platform_name = "momo購物")
sp_sam <- sp_train_data %>% 
  filter(timestamp >= p5 & timestamp < p6) %>% 
  group_by(item_id) %>% 
  summarise(pred_value = sum(total_sale)) %>%
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         year_month = paste0(year(p2 %m+% months(1)), "-", ifelse(month(p2 %m+% months(1)) < 10, paste0(0, month(p2 %m+% months(1))), month(p2 %m+% months(1)))),
         model = "sam_mon", platform_name = "蝦皮購物")
pc_sam <- pc_train_data %>% 
  filter(timestamp >= p5 & timestamp < p6) %>% 
  group_by(item_id) %>% 
  summarise(pred_value = sum(total_sale)) %>%
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         year_month = paste0(year(p2 %m+% months(1)), "-", ifelse(month(p2 %m+% months(1)) < 10, paste0(0, month(p2 %m+% months(1))), month(p2 %m+% months(1)))),
         model = "sam_mon", platform_name = "PChome購物")
ya_sam <- ya_train_data %>% 
  filter(timestamp >= p5 & timestamp < p6) %>% 
  group_by(item_id) %>% 
  summarise(pred_value = sum(total_sale)) %>%
  left_join(prod %>% mutate(QC = as.character(QC)), by = c("item_id" = "QC")) %>%
  mutate(parent_brand = case_when(parent_brand == "A-Derma艾芙美" ~ "統一藥品", parent_brand == "Avene雅漾" ~ "統一藥品", T ~ parent_brand), 
         year_month = paste0(year(p2 %m+% months(1)), "-", ifelse(month(p2 %m+% months(1)) < 10, paste0(0, month(p2 %m+% months(1))), month(p2 %m+% months(1)))),
         model = "sam_mon", platform_name = "Yahoo購物")

pred <- rbind(pred_arima, pred_ets, mo_thr, sp_thr, pc_thr, ya_thr, mo_six, sp_six, pc_six, ya_six, mo_sam, sp_sam, pc_sam, ya_sam)

cat('\nimport database:', as.character.POSIXt(Sys.time()),'\n')
pred = pred %>%
  mutate_at(names(which(sapply(pred, class) == 'character')),
            ~enc2native(.))
table_id = Id(schema = "tableau", table = "pred_platform_qty")
rs1 = dbWriteTable(msconn,
                   name = table_id,
                   row.names = F,
                   header = T,
                   value = pred,
                   append = T)
cat('\nINSERT', nrow(pred),'data, succeed:', rs1,'\n')
## END
g2 = as.character.POSIXt(Sys.time())
cat('\nTime difference of ',difftime(g2, g1, units = c("mins")),' mins\n')
## update JOB message ##
rs = dbSendQuery(msconn,paste0("
  UPDATE DataTeam.dbo.JOB_MESSAGE
  SET task_status = 1, end_time = '", force_tz(Sys.time(),"UTC"), "'
  WHERE task_name = '", job_msg$task_name, "'
  AND convert(varchar, start_time, 120) = '", job_msg$start_time, "'"))
dbClearResult(rs)
dbDisconnect(msconn)