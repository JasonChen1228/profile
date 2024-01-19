library(DBI)
library(stringr)
library(dplyr)
library(lubridate)
library(rfm)
library(ggplot2)
library(plotly)
library(keyring)
library(reshape2)
library(shiny)
library(shinydashboard)
library(htmlwidgets)
library(DT)
library(caTools)
library(scales)
library(car)
library(leaps)
library(glmnet)
# library(MASS)
msconn <- dbConnect(odbc::odbc(),
                    Driver = "ODBC Driver 17 for SQL SERVER",
                    Server = "10.0.130.225",
                    Database = "DataTeam",
                    UID = "jason_chen",
                    PWD = key_get("db_connect", "jason"),
                    Port = 1433)
# 排除取消的訂單、產品名稱：Thank you card、星巴克飲料券100元
# 最近一次的訂單寄送地址為居住城市
ap_sale <- dbGetQuery(msconn, "SELECT  x2.*, x1.order_id, x1.created_at, x1.order_day, x1.item_type, 
x1.QC, cp.parent_brand ,cp.sub_brand, x1.product_name, x1.quantity, x1.item_total, 
x1.total, x1.payment_type, x1.area, x1.deliver_platform   
FROM
(SELECT customer_id, order_id, DATEADD(HOUR, 8, created_at) AS created_at , CAST(DATEADD(HOUR, 8, created_at) AS DATE) AS order_day, item_type,
CASE WHEN product_name = 'Bioniq磁吸式洗漱牙膏架' THEN '1000048573' WHEN product_name = 'Plantur39多功能旅行收納盥洗包' THEN '1000041286' ELSE QC END AS QC, 
product_name, quantity, item_total, total, payment_type, 
CASE WHEN delivery_address_city IS NULL THEN LEFT(delivery_store_address,3) ELSE delivery_address_city END AS area, deliver_platform
FROM DataTeam.tableau.Alpecin_shopline_orders
WHERE product_name NOT IN ('Thank you card', '星巴克飲料券100元') 
AND order_status <> 'cancelled') x1
LEFT JOIN DataTeam.dbo.ce_product cp 
ON x1.QC = cp.QC 
LEFT JOIN (SELECT customer_id, 
CASE WHEN gender IS NULL THEN 'no_gender' ELSE gender END AS gender,
CASE 
WHEN birthday IS NULL THEN 'no_age'
WHEN YEAR(GETDATE()) - CAST(LEFT(birthday,4) AS INT) > 60 THEN '>60'
WHEN YEAR(GETDATE()) - CAST(LEFT(birthday,4) AS INT) > 50 THEN '51~60'
WHEN YEAR(GETDATE()) - CAST(LEFT(birthday,4) AS INT) > 40 THEN '41~50'
WHEN YEAR(GETDATE()) - CAST(LEFT(birthday,4) AS INT) > 30 THEN '31~40'
WHEN YEAR(GETDATE()) - CAST(LEFT(birthday,4) AS INT) > 20 THEN '21~30'
ELSE '<=20'
END AS age ,
CAST(DATEADD(HOUR, 8, create_date) AS DATE) AS join_day,
DATEDIFF(yyyy,CAST(DATEADD(HOUR, 8, create_date) AS DATE),GETDATE()) as join_year
FROM DataTeam.tableau.Alpecin_shopline_customers) x2
ON x1.customer_id = x2.customer_id ") %>% 
  mutate(area = case_when(area == "Taoyuan City" ~ "桃園市", area == "Taichung City" ~ "台中市", area == "New Taipei City" ~ "新北市", 
                          area == "Kaohsiung City" ~ "高雄市", area == "Taitung County" ~ "台東縣", area == "Hualien County" ~ "花蓮縣",
                          area == "Hsinchu County" ~ "新竹縣", area == "Chiayi City" ~ "嘉義市", area == "Taipei City" ~ "台北市", 
                          area == "Lan" ~ "桃園市", area == "Changhua County" ~ "彰化縣", T ~ area))
ap_sale <- ap_sale %>% filter(order_day <= "2023-08-31")
ap_area1 <- ap_sale %>% select(customer_id, created_at, area) %>% distinct() %>% 
  group_by(customer_id) %>% summarise(last_day = max(created_at)) %>% mutate(last_record = paste0(customer_id, "_", last_day))
ap_area2 <- ap_sale %>% select(customer_id, created_at, area) %>% distinct() %>% mutate(record = paste0(customer_id, "_", created_at))
ap_area3 <- ap_area2 %>% filter(record %in% ap_area1$last_record) %>% mutate(created_at = as.Date(created_at)) %>% select(customer_id, city = area)
ap_sale <- ap_sale %>% left_join(ap_area3, by = "customer_id") 

# 會員屬性
ap_mem <- dbGetQuery(msconn, "SELECT customer_id, 
CASE WHEN gender IS NULL THEN 'no_gender' ELSE gender END AS gender,
CASE 
WHEN birthday IS NULL THEN 'no_age'
WHEN YEAR(GETDATE()) - CAST(LEFT(birthday,4) AS INT) > 60 THEN '>60'
WHEN YEAR(GETDATE()) - CAST(LEFT(birthday,4) AS INT) > 50 THEN '51~60'
WHEN YEAR(GETDATE()) - CAST(LEFT(birthday,4) AS INT) > 40 THEN '41~50'
WHEN YEAR(GETDATE()) - CAST(LEFT(birthday,4) AS INT) > 30 THEN '31~40'
WHEN YEAR(GETDATE()) - CAST(LEFT(birthday,4) AS INT) > 20 THEN '21~30'
ELSE '<=20'
END AS age ,
CAST(DATEADD(HOUR, 8, create_date) AS DATE) AS join_day,
DATEDIFF(yyyy,CAST(DATEADD(HOUR, 8, create_date) AS DATE),GETDATE()) as join_year
FROM DataTeam.tableau.Alpecin_shopline_customers") %>% 
  left_join(ap_sale %>% select(customer_id, city) %>% distinct(), by = "customer_id") %>%
  mutate(city = ifelse(is.na(city) == T, "no_city", city)) %>% filter(join_day <= "2023-08-31")


# 回購率
# 該年有購買2次以上的用戶數/該年有購買的用戶數(購買次數-購買日期與會員ID當作唯一值計算)，後續以sub_brand當作商品類別進行計算回購率
customer_repurchase_rate <- c()
for(i in unique(year(ap_sale$order_day))){
  purchase_id <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% select(customer_id, order_day) %>% distinct() %>% group_by(customer_id) %>% summarise(buy_count = n()) %>% filter(buy_count > 1) %>% nrow()
  item_buy <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% group_by(year = year(order_day)) %>% summarise(n = n_distinct(customer_id)) %>% select(n)
  a1 <- as.numeric(purchase_id/item_buy)
  customer_repurchase_rate <- c(customer_repurchase_rate, paste0(i, "-", a1))
}
customer_repurchase_rate <- data.frame(str_split_fixed(customer_repurchase_rate, pattern = "-", n = 2))
colnames(customer_repurchase_rate) <- c("year", "customer_repurchase_rate")
customer_repurchase_rate <- customer_repurchase_rate %>% mutate(year = as.integer(year), customer_repurchase_rate = as.double(customer_repurchase_rate))
ap_shampoo_rp <- c()
for(i in unique(year(ap_sale$order_day))){
  repurchase_ap_shampoo_counts <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% select(customer_id, order_day, sub_brand) %>% 
    distinct() %>% group_by(customer_id, sub_brand) %>% summarise(buy_count = n()) %>% filter(buy_count > 1) %>% 
    group_by(sub_brand) %>% summarise(n = n()) %>% filter(sub_brand == "Alpecin洗髮露") %>% select(n)
if(nrow(repurchase_ap_shampoo_counts) > 0){
  purchase_ap_shampoo_counts <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% group_by(sub_brand) %>% summarise(n = n_distinct(customer_id)) %>% filter(sub_brand == "Alpecin洗髮露") %>% select(n)
  a1 <- as.numeric(repurchase_ap_shampoo_counts/purchase_ap_shampoo_counts)
  ap_shampoo_rp <- c(ap_shampoo_rp, paste0(i, "-", a1))
}else{
  next
}
}
ap_shampoo_rp <- data.frame(str_split_fixed(ap_shampoo_rp, pattern = "-", n = 2))
colnames(ap_shampoo_rp) <- c("year", "ap_shampoo_rp")
ap_shampoo_rp <- ap_shampoo_rp %>% mutate(year = as.integer(year), ap_shampoo_rp = as.double(ap_shampoo_rp))
ap_hair_rp <- c()
for(i in unique(year(ap_sale$order_day))){
  repurchase_ap_hair_counts <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% select(customer_id, order_day, sub_brand) %>% 
    distinct() %>% group_by(customer_id, sub_brand) %>% summarise(buy_count = n()) %>% filter(buy_count > 1) %>% 
    group_by(sub_brand) %>% summarise(n = n()) %>% filter(sub_brand == "Alpecin-頭髮液") %>% select(n)
  if(nrow(repurchase_ap_hair_counts) > 0){
    purchase_ap_hair_counts <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% group_by(sub_brand) %>% summarise(n = n_distinct(customer_id)) %>% filter(sub_brand == "Alpecin-頭髮液") %>% select(n)
    a1 <- as.numeric(repurchase_ap_hair_counts/purchase_ap_hair_counts)
    ap_hair_rp <- c(ap_hair_rp, paste0(i, "-", a1))
  }else{
    next
  }
}
ap_hair_rp <- data.frame(str_split_fixed(ap_hair_rp, "-", n = 2))
colnames(ap_hair_rp) <- c("year", "ap_hair_rp")
ap_hair_rp <- ap_hair_rp %>% mutate(year = as.integer(year), ap_hair_rp = as.double(ap_hair_rp))
ap_bio_rp <- c()
for(i in unique(year(ap_sale$order_day))){
  repurchase_ap_bio_counts <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% select(customer_id, order_day, sub_brand) %>% 
    distinct() %>% group_by(customer_id, sub_brand) %>% summarise(buy_count = n()) %>% filter(buy_count > 1) %>% 
    group_by(sub_brand) %>% summarise(n = n()) %>% filter(sub_brand == "Bioniq 貝歐尼") %>% select(n)
  if(nrow(repurchase_ap_bio_counts) > 0){
    purchase_ap_bio_counts <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% group_by(sub_brand) %>% summarise(n = n_distinct(customer_id)) %>% filter(sub_brand == "Bioniq 貝歐尼") %>% select(n)
    a1 <- as.numeric(repurchase_ap_bio_counts/purchase_ap_bio_counts)
    ap_bio_rp <- c(ap_bio_rp, paste0(i, "-", a1))
  }else{
    next
  }
}
ap_bio_rp <- data.frame(str_split_fixed(ap_bio_rp, pattern = "-", n = 2))
colnames(ap_bio_rp) <- c("year", "ap_bio_rp")
ap_bio_rp <- ap_bio_rp %>% mutate(year = as.integer(year), ap_bio_rp = as.double(ap_bio_rp))
ap_p21_rp <- c()
for(i in unique(year(ap_sale$order_day))){
  repurchase_ap_p21_counts <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% select(customer_id, order_day, sub_brand) %>% 
    distinct() %>% group_by(customer_id, sub_brand) %>% summarise(buy_count = n()) %>% filter(buy_count > 1) %>% 
    group_by(sub_brand) %>% summarise(n = n()) %>% filter(sub_brand == "Plantur21") %>% select(n)
  if(nrow(repurchase_ap_p21_counts > 0)){
    purchase_ap_p21_counts <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% group_by(sub_brand) %>% summarise(n = n_distinct(customer_id)) %>% filter(sub_brand == "Plantur21") %>% select(n)
    a1 <- as.numeric(repurchase_ap_p21_counts/purchase_ap_p21_counts)
    ap_p21_rp <- c(ap_p21_rp, paste0(i, "-", a1))
  }else{
    next
  }
}
ap_p21_rp <- data.frame(str_split_fixed(ap_p21_rp, pattern = "-", n = 2))
colnames(ap_p21_rp) <- c("year", "ap_p21_rp")
ap_p21_rp <- ap_p21_rp %>% mutate(year = as.integer(year), ap_p21_rp = as.double(ap_p21_rp))
ap_p39_shampoo_rp <- c()
for(i in unique(year(ap_sale$order_day))){
  repurchase_ap_p39_shampoo_counts <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% select(customer_id, order_day, sub_brand) %>% 
    distinct() %>% group_by(customer_id, sub_brand) %>% summarise(buy_count = n()) %>% filter(buy_count > 1) %>% 
    group_by(sub_brand) %>% summarise(n = n()) %>% filter(sub_brand == "Plantur39-洗髮露") %>% select(n)
  if(nrow(repurchase_ap_p39_shampoo_counts) > 0){
    purchase_ap_p39_shampoo_counts <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% group_by(sub_brand) %>% summarise(n = n_distinct(customer_id)) %>% filter(sub_brand == "Plantur39-洗髮露") %>% select(n)
    a1 <- as.numeric(repurchase_ap_p39_shampoo_counts/purchase_ap_p39_shampoo_counts)
    ap_p39_shampoo_rp <- c(ap_p39_shampoo_rp, paste0(i, "-", a1))
  }else{
    next
  }
}
ap_p39_shampoo_rp <- data.frame(str_split_fixed(ap_p39_shampoo_rp, pattern = "-", n = 2))
colnames(ap_p39_shampoo_rp) <- c("year", "ap_p39_shampoo_rp")
ap_p39_shampoo_rp <- ap_p39_shampoo_rp %>% mutate(year = as.integer(year), ap_p39_shampoo_rp = as.double(ap_p39_shampoo_rp))
ap_p39_hair_rp <- c()
for(i in unique(year(ap_sale$order_day))){
  repurchase_ap_p39_hair_counts <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% select(customer_id, order_day, sub_brand) %>% 
    distinct() %>% group_by(customer_id, sub_brand) %>% summarise(buy_count = n()) %>% filter(buy_count > 1) %>% 
    group_by(sub_brand) %>% summarise(n = n()) %>% filter(sub_brand == "Plantur39-頭髮液") %>% select(n)
  if(nrow(repurchase_ap_p39_hair_counts) > 0){
    purchase_ap_p39_hair_counts <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% group_by(sub_brand) %>% summarise(n = n_distinct(customer_id)) %>% filter(sub_brand == "Plantur39-頭髮液") %>% select(n)
    a1 <- as.numeric(repurchase_ap_p39_hair_counts/purchase_ap_p39_hair_counts)
    ap_p39_hair_rp <- c(ap_p39_hair_rp, paste0(i, "-", a1))
  }else{
    next
  }
}
ap_p39_hair_rp <- data.frame(str_split_fixed(ap_p39_hair_rp, pattern = "-", n = 2))
colnames(ap_p39_hair_rp) <- c("year", "ap_p39_hair_rp")
ap_p39_hair_rp <- ap_p39_hair_rp %>% mutate(year = as.integer(year), ap_p39_hair_rp = as.double(ap_p39_hair_rp))
ap_repurchase <- Reduce(function(x,y) left_join(x, y, by = "year"), list(data.frame(year = unique(year(ap_sale$order_day))), customer_repurchase_rate, ap_shampoo_rp, ap_hair_rp, ap_bio_rp, ap_p21_rp, ap_p39_shampoo_rp, ap_p39_hair_rp)) %>% mutate_all(., ~replace(., is.na(.), 0))

# 回購天數
# 先篩選出該年有購買2次以上的用戶數，計算各自購買日期之時間差，再取其中位數當作其回購天數
customer_rp_day <- c()
for(i in unique(year(ap_sale$order_day))){
  purchase_id <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% select(customer_id, order_day) %>% 
    distinct() %>% group_by(customer_id) %>% summarise(buy_count = n()) %>% filter(buy_count > 1) 
  day_gap <- ap_sale %>% filter(customer_id %in% purchase_id$customer_id & as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% 
    select(customer_id, order_day) %>% distinct() %>% arrange(customer_id, order_day) %>%
    group_by(customer_id) %>% mutate(purchase_interval = order_day - lag(order_day)) %>% filter(is.na(purchase_interval) == F) %>%
    group_by(customer_id) %>% summarise(purchase_interval = median(purchase_interval))
  customer_rp_day <- c(customer_rp_day, paste0(i, "_", median(day_gap$purchase_interval)))
}
customer_rp_day <- data.frame(str_split_fixed(customer_rp_day, pattern = "_", n = 2))
colnames(customer_rp_day) <- c("year", "消費者")
customer_rp_day <- customer_rp_day %>% mutate(year = as.integer(year), 消費者 = as.double(消費者))
ap_shampoo_rp_day <- c()
for(i in unique(year(ap_sale$order_day))){
  purchase_ap_shampoo_id <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% select(customer_id, order_day, sub_brand) %>% 
    distinct() %>% group_by(customer_id, sub_brand) %>% summarise(buy_count = n()) %>% filter(buy_count > 1 & sub_brand == "Alpecin洗髮露")
  ap_shampoo_gap <- ap_sale %>% filter(customer_id %in% purchase_ap_shampoo_id$customer_id & sub_brand == "Alpecin洗髮露" & as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% 
    select(customer_id, order_day) %>% distinct() %>% arrange(customer_id, order_day) %>%
    group_by(customer_id) %>% mutate(purchase_interval = order_day - lag(order_day)) %>% filter(is.na(purchase_interval) == F) %>%
    group_by(customer_id) %>% summarise(purchase_interval = median(purchase_interval))
  ap_shampoo_rp_day <- c(ap_shampoo_rp_day, paste0(i, "_", median(ap_shampoo_gap$purchase_interval)))
}
ap_shampoo_rp_day <- data.frame(str_split_fixed(ap_shampoo_rp_day, pattern = "_", n = 2))
colnames(ap_shampoo_rp_day) <- c("year", "Alpecin洗髮露")
ap_shampoo_rp_day <- ap_shampoo_rp_day %>% mutate(year = as.integer(year), Alpecin洗髮露 = as.double(Alpecin洗髮露))
ap_hair_rp_day <- c()
for(i in unique(year(ap_sale$order_day))){
  purchase_ap_hair_id <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% select(customer_id, order_day, sub_brand) %>% 
    distinct() %>% group_by(customer_id, sub_brand) %>% summarise(buy_count = n()) %>% filter(buy_count > 1 & sub_brand == "Alpecin-頭髮液")
  ap_hair_gap <- ap_sale %>% filter(customer_id %in% purchase_ap_hair_id$customer_id & sub_brand == "Alpecin-頭髮液" & as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% 
    select(customer_id, order_day) %>% distinct() %>% arrange(customer_id, order_day) %>%
    group_by(customer_id) %>% mutate(purchase_interval = order_day - lag(order_day)) %>% filter(is.na(purchase_interval) == F) %>%
    group_by(customer_id) %>% summarise(purchase_interval = median(purchase_interval))
  ap_hair_rp_day <- c(ap_hair_rp_day, paste0(i, "_", median(ap_hair_gap$purchase_interval)))
}
ap_hair_rp_day <- data.frame(str_split_fixed(ap_hair_rp_day, pattern = "_", n = 2))
colnames(ap_hair_rp_day) <- c("year", "Alpecin頭髮液")
ap_hair_rp_day <- ap_hair_rp_day %>% mutate(year = as.integer(year), Alpecin頭髮液 = as.double(Alpecin頭髮液))
ap_bio_rp_day <- c()
for(i in unique(year(ap_sale$order_day))){
  purchase_ap_bio_id <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% select(customer_id, order_day, sub_brand) %>% 
    distinct() %>% group_by(customer_id, sub_brand) %>% summarise(buy_count = n()) %>% filter(buy_count > 1 & sub_brand == "Bioniq 貝歐尼")
  ap_bio_gap <- ap_sale %>% filter(customer_id %in% purchase_ap_bio_id$customer_id & sub_brand == "Bioniq 貝歐尼" & as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% 
    select(customer_id, order_day) %>% distinct() %>% arrange(customer_id, order_day) %>%
    group_by(customer_id) %>% mutate(purchase_interval = order_day - lag(order_day)) %>% filter(is.na(purchase_interval) == F) %>%
    group_by(customer_id) %>% summarise(purchase_interval = median(purchase_interval))
  ap_bio_rp_day <- c(ap_bio_rp_day, paste0(i, "_", median(ap_bio_gap$purchase_interval)))
}
ap_bio_rp_day <- data.frame(str_split_fixed(ap_bio_rp_day, pattern = "_", n = 2))
colnames(ap_bio_rp_day) <- c("year", "Bioniq_貝歐尼")
ap_bio_rp_day <- ap_bio_rp_day %>% mutate(year = as.integer(year), Bioniq_貝歐尼 = as.double(Bioniq_貝歐尼))
ap_p21_rp_day <- c()
for(i in unique(year(ap_sale$order_day))){
  purchase_ap_p21_id <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% select(customer_id, order_day, sub_brand) %>% 
    distinct() %>% group_by(customer_id, sub_brand) %>% summarise(buy_count = n()) %>% filter(buy_count > 1 & sub_brand == "Plantur21")
  ap_p21_gap <- ap_sale %>% filter(customer_id %in% purchase_ap_p21_id$customer_id & sub_brand == "Plantur21" & as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% 
    select(customer_id, order_day) %>% distinct() %>% arrange(customer_id, order_day) %>%
    group_by(customer_id) %>% mutate(purchase_interval = order_day - lag(order_day)) %>% filter(is.na(purchase_interval) == F) %>%
    group_by(customer_id) %>% summarise(purchase_interval = median(purchase_interval))
  ap_p21_rp_day <- c(ap_p21_rp_day, paste0(i, "_", median(ap_p21_gap$purchase_interval)))
}
ap_p21_rp_day <- data.frame(str_split_fixed(ap_p21_rp_day, pattern = "_", n = 2))
colnames(ap_p21_rp_day) <- c("year", "Plantur21")
ap_p21_rp_day <- ap_p21_rp_day %>% mutate(year = as.integer(year), Plantur21 = as.double(Plantur21))
ap_p39_shampoo_rp_day <- c()
for(i in unique(year(ap_sale$order_day))){
  purchase_ap_p39_shampoo_id <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% select(customer_id, order_day, sub_brand) %>% 
    distinct() %>% group_by(customer_id, sub_brand) %>% summarise(buy_count = n()) %>% filter(buy_count > 1 & sub_brand == "Plantur39-洗髮露")
  ap_p39_shampoo_gap <- ap_sale %>% filter(customer_id %in% purchase_ap_p39_shampoo_id$customer_id & sub_brand == "Plantur39-洗髮露" & as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% 
    select(customer_id, order_day) %>% distinct() %>% arrange(customer_id, order_day) %>%
    group_by(customer_id) %>% mutate(purchase_interval = order_day - lag(order_day)) %>% filter(is.na(purchase_interval) == F) %>%
    group_by(customer_id) %>% summarise(purchase_interval = median(purchase_interval))
  ap_p39_shampoo_rp_day <- c(ap_p39_shampoo_rp_day, paste0(i, "_", median(ap_p39_shampoo_gap$purchase_interval)))
}
ap_p39_shampoo_rp_day <- data.frame(str_split_fixed(ap_p39_shampoo_rp_day, pattern = "_", n = 2))
colnames(ap_p39_shampoo_rp_day) <- c("year", "Plantur39_洗髮露")
ap_p39_shampoo_rp_day <- ap_p39_shampoo_rp_day %>% mutate(year = as.integer(year), Plantur39_洗髮露 = as.double(Plantur39_洗髮露))
ap_p39_hair_rp_day <- c()
for(i in unique(year(ap_sale$order_day))){
  purchase_ap_p39_hair_id <- ap_sale %>% filter(as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% select(customer_id, order_day, sub_brand) %>% 
    distinct() %>% group_by(customer_id, sub_brand) %>% summarise(buy_count = n()) %>% filter(buy_count > 1 & sub_brand == "Plantur39-頭髮液")
  ap_p39_hair_gap <- ap_sale %>% filter(customer_id %in% purchase_ap_p39_hair_id$customer_id & sub_brand == "Plantur39-頭髮液" & as.Date(paste0(i,"-01-01")) <= order_day & order_day <= as.Date(paste0(i,"-12-31"))) %>% 
    select(customer_id, order_day) %>% distinct() %>% arrange(customer_id, order_day) %>%
    group_by(customer_id) %>% mutate(purchase_interval = order_day - lag(order_day)) %>% filter(is.na(purchase_interval) == F) %>%
    group_by(customer_id) %>% summarise(purchase_interval = median(purchase_interval))
  ap_p39_hair_rp_day <- c(ap_p39_hair_rp_day, paste0(i, "_", median(ap_p39_hair_gap$purchase_interval)))
}
ap_p39_hair_rp_day <- data.frame(str_split_fixed(ap_p39_hair_rp_day, pattern = "_", n = 2))
colnames(ap_p39_hair_rp_day) <- c("year", "Plantur39_頭髮液")
ap_p39_hair_rp_day <- ap_p39_hair_rp_day %>% mutate(year = as.integer(year), Plantur39_頭髮液 = as.double(Plantur39_頭髮液))
ap_rp_day <- Reduce(function(x,y) left_join(x, y, by = "year"), list(data.frame(year = unique(year(ap_sale$order_day))), customer_rp_day, ap_shampoo_rp_day, ap_hair_rp_day, ap_bio_rp_day, ap_p21_rp_day, ap_p39_shampoo_rp_day, ap_p39_hair_rp_day)) %>% mutate_all(., ~replace(., is.na(.), 0))


# 
# 會員分群-RFM
# 依照各用戶的訂單(最近一次購買日期、購買金額、購買次數)進行計算RFM分數，每個標準分成2大類，共分成八組，儀表版新增依購買年分或不分年分計算RFM
# RFM  type
# HHH	高價值活躍     >>>複購率高、購買頻次高、花費金額大的客戶，是價值最大的用戶 (主力客戶，應確保他們的客戶滿意度和維持客戶關係)
# HLH	重要發展       >>>買的多、買的貴但是不常買的客戶，我們要重點保持 (潛在的高價值活躍戶，行銷主要對象)
# LHH	高價值潛在流失 >>>經常買、花費大但是購買頻次不多的客戶，我們要發展其多購買 (刺激此用戶群，引導成為高價值活躍用戶)
# LLH	高價值流失     >>>願意花錢但是不常買、購買頻次不多的客戶，我們要重點挽留 (主力挽回用戶群，應為行銷預算的主要投入群)
# HHL	低價值活躍     >>>複購率高、購買頻次高，但是花費金額小的客戶，屬於一般價值
# HLL	一般用戶       >>>經常買，但是買不多、花錢也不多，屬於一般發展客戶
# LHL	低價值潛在流失 >>>買的多但是不常買、花錢不多，屬於一般保持客戶
# LLL	低價值流失     >>>不願花錢、不常買、購買頻次不高，最沒有價值的客戶
rfm_fun <- function(mem_year){
  if(mem_year == "All"){
    rfm_data <- ap_sale %>%
      select(customer_id, order_id, order_day, total) %>%
      distinct() %>%
      group_by(customer_id) %>%
      summarise(
        recency = as.numeric(Sys.Date()) - as.numeric(max(order_day)), 
        frequency = n_distinct(order_day), 
        monetary = sum(total) 
      )
    rfm_table <- rfm_table_customer(data = rfm_data, customer_id = customer_id, n_transactions = frequency, recency_days = recency,
                                    total_revenue = monetary, recency_bins = 4, frequency_bins = 4, monetary_bins = 4)
    rfm_result <- rfm_table$rfm
    rfm_result <- rfm_result %>% mutate(recency_type = ifelse(recency_score > 2, "H","L"),
                                        frequency_type = ifelse(frequency_score > 2, "H","L"),
                                        monetary_type = ifelse(monetary_score > 2, "H", "L"), 
                                        rfm_type = paste0(recency_type, frequency_type, monetary_type),
                                        rfm_type = case_when(rfm_type == "HHH" ~ "高價值活躍",
                                                             rfm_type == "HLH" ~ "重要發展",
                                                             rfm_type == "LHH" ~ "高價值潛在流失",
                                                             rfm_type == "LLH" ~ "高價值流失",
                                                             rfm_type == "HHL" ~ "低價值活躍",
                                                             rfm_type == "HLL" ~ "一般用戶",
                                                             rfm_type == "LHL" ~ "低價值潛在流失",
                                                             rfm_type == "LLL" ~ "低價值流失")) 
    # rb_mem <- rb_sale %>% select(member_id, age, gender, area, join_year) %>% distinct() %>% select(member_id, age, gender, area, join_year)
    rfm_result <- left_join(rfm_result, ap_mem, by = c("customer_id"))
    return(rfm_result)
  }else{
    rfm_data <- ap_sale %>% 
      filter(join_day >= as.Date(paste(mem_year,"01-01", sep = "-")) & join_day <= as.Date(paste(mem_year,"12-31", sep = "-"))) %>%
      select(customer_id, order_id, order_day, total) %>%
      distinct() %>%
      group_by(customer_id) %>%
      summarise(
        recency = as.numeric(Sys.Date()) - as.numeric(max(order_day)), 
        frequency = n_distinct(order_day), 
        monetary = sum(total) 
      )
    rfm_table <- rfm_table_customer(data = rfm_data, customer_id = customer_id, n_transactions = frequency, recency_days = recency,
                                    total_revenue = monetary, recency_bins = 4, frequency_bins = 4, monetary_bins = 4)
    rfm_result <- rfm_table$rfm
    rfm_result <- rfm_result %>% mutate(recency_type = ifelse(recency_score > 2, "H","L"),
                                        frequency_type = ifelse(frequency_score > 2, "H","L"),
                                        monetary_type = ifelse(monetary_score > 2, "H", "L"), 
                                        rfm_type = paste0(recency_type, frequency_type, monetary_type),
                                        rfm_type = case_when(rfm_type == "HHH" ~ "高價值活躍",
                                                             rfm_type == "HLH" ~ "重要發展",
                                                             rfm_type == "LHH" ~ "高價值潛在流失",
                                                             rfm_type == "LLH" ~ "高價值流失",
                                                             rfm_type == "HHL" ~ "低價值活躍",
                                                             rfm_type == "HLL" ~ "一般用戶",
                                                             rfm_type == "LHL" ~ "低價值潛在流失",
                                                             rfm_type == "LLL" ~ "低價值流失")) 
    # rb_mem <- rb_sale %>% select(member_id, age, gender, area, join_year) %>% distinct() %>% select(member_id, age, gender, area, join_year)
    rfm_result <- left_join(rfm_result, ap_mem, by = c("customer_id"))
    return(rfm_result)
  } 
}

# NAPL
# 購買週期則是從今日回推365天進行計算
# 先切成1次購、2次購的會員，然後再依據最近購物時間，3個月內(購買週期)第一次購物的是N（New），買過一次超過3個月(購買週期)沒買第二次是L（Lost）。
# 3個月內有購物+2次購以上的是A（Active），這一群活躍客是最有價值的一群會員。
# 買過2次+近三個月都沒有購物的，就會掉到P（Potential）。
# NAPL四群會員是彼此不重複的，而事實上，NAPL四群的人數加起來是該品牌「一年內有消費的總會員數」。
# 兩群會員是R（Ready to buy），是註冊後從來沒有消費過的會員；另一群是S（Sealed），也就是雖然消費過，但已經超過一年沒有回來買了的會員，是一種「封存」的會員。
napl_fun <- function(date1, dat){
  # 購買次數(order_day)>1的會員
  napl_id <- ap_sale %>% filter(order_day >= (date1-365) & order_day <= date1) %>% select(customer_id, order_day) %>% distinct() %>% group_by(customer_id) %>% summarise(buy_count = n()) %>% filter(buy_count > 1)
  # 購買次數>1的會員-購買週期
  napl_table2 <- ap_sale %>% filter(order_day >= (date1-365) & order_day <= date1 & customer_id %in% napl_id$customer_id) %>% 
    select(customer_id, order_day) %>% distinct() %>% arrange(customer_id, order_day) %>%
    group_by(customer_id) %>% mutate(purchase_interval = order_day - lag(order_day)) %>% filter(is.na(purchase_interval) == F) %>%
    group_by(customer_id) %>% summarise(purchase_interval = median(purchase_interval))
  # NAPL分類
  napl_table1 <- ap_sale %>% 
    filter(order_day >= (date1-365) & order_day <= date1) %>% 
    select(customer_id, order_id, order_day, total) %>% distinct() %>% group_by(customer_id) %>%
    summarise( recency = as.numeric(date1)-as.numeric(max(order_day)), frequency = n_distinct(order_day), monetary = sum(total)) %>%
    mutate(freq_lable = ifelse(frequency > 1, "multiple", "single"), 
           rece_lable = ifelse(recency < 3*median(napl_table2$purchase_interval), "new", "lost"),
           type = paste(freq_lable, rece_lable, sep = "_"),
           napl_type = case_when(type == "single_new" ~ "N",
                                 type == "single_lost" ~ "L",
                                 type == "multiple_new" ~ "A",
                                 type == "multiple_lost" ~ "P"))
  # 會員屬性
  napl_mem <- ap_mem %>% select(customer_id, age, gender, city, join_year) %>% distinct() %>% select(customer_id, age, gender, city, join_year)
  napl_table1 <- napl_table1 %>% left_join(napl_mem, by = "customer_id")
  # 先前消費過的會員
  pre_sale_mem <- ap_sale %>% filter(order_day <= (date1-365)) %>% select(customer_id, age) %>% distinct()
  # R-註冊後從來沒有消費過的會員
  npal_r_mem <- ap_mem %>% filter(join_day <= date1) %>% select(customer_id, age, gender, city, join_year) %>% mutate(napl_type = "R") %>% 
    filter(!customer_id %in% unique(napl_table1$customer_id) & !customer_id %in% unique(pre_sale_mem$customer_id)) %>% distinct()
  # S-雖然消費過已經超過一年沒有回來買了的會員，是一種「封存」的會員
  npal_s_mem <- ap_mem %>% filter(join_year <= date1) %>% select(customer_id, age, gender, city, join_year) %>% mutate(napl_type = "S") %>% 
    select(customer_id, age, gender, city, join_year, napl_type) %>% filter(!customer_id %in% unique(npal_r_mem$customer_id) & !customer_id %in% unique(napl_table1$customer_id)) %>% distinct()
  npal_rs_mem <- rbind(npal_s_mem, npal_r_mem)
  napl_total <- napl_table1 %>% select(customer_id, age, gender, city, join_year, napl_type) %>% rbind(npal_rs_mem)
  if(dat == 1){
    return(napl_total)
  }else{
    return(napl_table1)
  }
}

# 2021顧客僅50位，其中僅6位於2022年繼續購買，故模型不準確，等後續收集更多資料時再做查看，此結果參考使用 >>> 2022年有消費用戶的預估留存率
# 保留率
retain <- function(order_year){
  retain_rfm_data <- ap_sale %>% filter(order_day >= as.Date(paste0(order_year-2,"-01-01")) & order_day <= as.Date(paste0(order_year-2,"-12-31"))) %>% 
    select(customer_id, order_id, order_day, total) %>% distinct() %>% group_by(customer_id) %>%
    summarise(recency = as.numeric(as.Date(paste0(order_year-2,"-12-31"))) - as.numeric(max(order_day)), frequency = n_distinct(order_day), monetary = sum(total)) %>% 
    rfm_table_customer(customer_id = customer_id, n_transactions = frequency, recency_days = recency, total_revenue = monetary, recency_bins = 4, frequency_bins = 4, monetary_bins = 4)
  retain_train_rfm <- retain_rfm_data$rfm %>% mutate(recency_type = ifelse(recency_score > 2, "H","L"),
                                                     frequency_type = ifelse(frequency_score > 2, "H","L"),
                                                     monetary_type = ifelse(monetary_score > 2, "H", "L"), 
                                                     rfm_type = paste0(recency_type, frequency_type, monetary_type),
                                                     rfm_type = case_when(rfm_type == "HHH" ~ "高價值活躍",
                                                                          rfm_type == "HLH" ~ "重要發展",
                                                                          rfm_type == "LHH" ~ "高價值潛在流失",
                                                                          rfm_type == "LLH" ~ "高價值流失",
                                                                          rfm_type == "HHL" ~ "低價值活躍",
                                                                          rfm_type == "HLL" ~ "一般用戶",
                                                                          rfm_type == "LHL" ~ "低價值潛在流失",
                                                                          rfm_type == "LLL" ~ "低價值流失"))
  next_year <- ap_sale %>% filter(order_day >= as.Date(paste0(order_year-1,"-01-01")) & order_day <= as.Date(paste0(order_year-1,"-12-31"))) %>%select(customer_id, order_id, order_day, total) %>% distinct() %>% group_by(customer_id) %>% summarise(frequency = n_distinct(order_day))
  train_data <- retain_train_rfm %>% left_join(next_year, by = c("customer_id")) %>% 
    mutate(Retain = case_when(frequency > 0 ~ 1, T ~ 0)) %>% select(customer_id, recency_days, transaction_count,amount, rfm_type, Retain) %>%
    left_join(ap_mem, by = c("customer_id")) %>% mutate(rfm_type = as.factor(rfm_type),
                                                                      age = as.factor(age),
                                                                      gender = case_when(gender == 'female' ~ 'female',
                                                                                         gender == 'male' ~ 'male',
                                                                                         T ~ 'no_gender'),
                                                                      gender = as.factor(gender),
                                                                      city =  case_when(city %in% c("臺北市", "新北市", "基隆市", "新竹市", "桃園市", "新竹縣", "宜蘭縣") == T ~ "北部",
                                                                                        city %in% c("臺中市", "苗栗縣", "彰化縣", "南投縣", "雲林縣") == T ~ "中部",
                                                                                        city %in% c("高雄市", "臺南市", "嘉義市", "嘉義縣", "屏東縣", "澎湖縣") == T ~ "南部",
                                                                                        city %in% c("花蓮縣", "臺東縣") == T ~ "東部",
                                                                                        T ~ "no_city"),
                                                                      city = as.factor(city),
                                                                      Retain = as.factor(Retain)) %>% select(-join_day)
  train_data1 <- train_data %>% select(-customer_id,-join_year)
  model_retain <- glm(Retain ~ ., train_data1, family = binomial())
  # 逐步回歸，使用stepAIC進行逐步選擇變數，direction = "both"表示可以同時添加或刪除變數
  step_model <- MASS::stepAIC(model_retain, direction = "both", trace = F)
  retain_vif <- vif(step_model)
  while(max(retain_vif[,1]) > 4){
    model_retain1 <- glm(Retain ~ ., train_data1 %>% select(-rownames(retain_vif)[which.max(retain_vif[,1])]), family = binomial())
    retain_vif <- vif(model_retain1)
  }
  retain_rfm_test_data <- ap_sale %>% filter(order_day >= as.Date(paste0(order_year-1,"-01-01")) & order_day <= as.Date(paste0(order_year-1,"-12-31"))) %>% 
    select(customer_id, order_id, order_day, total) %>% distinct() %>% group_by(customer_id) %>%
    summarise(recency = as.numeric(as.Date(paste0(order_year-1,"-12-31"))) - as.numeric(max(order_day)), frequency = n_distinct(order_day), monetary = sum(total)) %>% 
    rfm_table_customer(customer_id = customer_id, n_transactions = frequency, recency_days = recency, total_revenue = monetary, recency_bins = 4, frequency_bins = 4, monetary_bins = 4)
  retain_test_rfm <- retain_rfm_test_data$rfm %>% mutate(recency_type = ifelse(recency_score > 2, "H","L"),
                                                         frequency_type = ifelse(frequency_score > 2, "H","L"),
                                                         monetary_type = ifelse(monetary_score > 2, "H", "L"), 
                                                         rfm_type = paste0(recency_type, frequency_type, monetary_type),
                                                         rfm_type = case_when(rfm_type == "HHH" ~ "高價值活躍",
                                                                              rfm_type == "HLH" ~ "重要發展",
                                                                              rfm_type == "LHH" ~ "高價值潛在流失",
                                                                              rfm_type == "LLH" ~ "高價值流失",
                                                                              rfm_type == "HHL" ~ "低價值活躍",
                                                                              rfm_type == "HLL" ~ "一般用戶",
                                                                              rfm_type == "LHL" ~ "低價值潛在流失",
                                                                              rfm_type == "LLL" ~ "低價值流失"))
  retain_test_data <- retain_test_rfm %>% left_join(ap_mem, by = "customer_id") %>% mutate(rfm_type = as.factor(rfm_type), 
                                                                                                            age = as.factor(age), 
                                                                                                            gender = case_when(gender == 'female' ~ 'female',
                                                                                                                               gender == 'male' ~ 'male',
                                                                                                                               T ~ 'no_gender'),
                                                                                                            gender = as.factor(gender),  
                                                                                                            city =  case_when(city %in% c("臺北市", "新北市", "基隆市", "新竹市", "桃園市", "新竹縣", "宜蘭縣") == T ~ "北部",
                                                                                                                              city %in% c("臺中市", "苗栗縣", "彰化縣", "南投縣", "雲林縣") == T ~ "中部",
                                                                                                                              city %in% c("高雄市", "臺南市", "嘉義市", "嘉義縣", "屏東縣", "澎湖縣") == T ~ "南部",
                                                                                                                              city %in% c("花蓮縣", "臺東縣") == T ~ "東部",
                                                                                                                              T ~ "no_city"), 
                                                                                                            city = as.factor(city)) %>% select(customer_id, recency_days, transaction_count, amount, rfm_type, age, gender, city, join_year)
  retain_test_data1 <- retain_test_data %>% mutate(city = as.character(city), city = ifelse(city %in% unique(train_data1$city) == T, city, "no_city"), city = as.factor(city))
  pred_test <- predict(model_retain1, newdata = retain_test_data1[,2:ncol(retain_test_data1)], type = "response")
  retain_result <- cbind(retain_test_data, pred_test)
  return(retain_result)
}

# 購買金額
# 2021顧客僅50位，其中僅6位於2022年繼續購買，故模型不準確，等後續收集更多資料時再做查看，此結果參考使用 >>> 2022年有消費用戶的預估購買金額
purchase <- function(order_year){
  purchase_train_data <- ap_sale %>% filter(order_day >= as.Date(paste0(order_year-2,"-01-01")) & order_day <= as.Date(paste0(order_year-2,"-12-31"))) %>% 
    select(customer_id, order_id, order_day, total) %>% distinct() %>% group_by(customer_id) %>%
    summarise(recency = as.numeric(as.Date(paste0(order_year-2,"-12-31"))) - as.numeric(max(order_day)), frequency = n_distinct(order_day), monetary = sum(total)) %>% 
    rfm_table_customer(customer_id = customer_id, n_transactions = frequency, recency_days = recency, total_revenue = monetary, recency_bins = 4, frequency_bins = 4, monetary_bins = 4)
  purchase_train_rfm <- purchase_train_data$rfm %>% mutate(recency_type = ifelse(recency_score > 2, "H","L"),
                                                           frequency_type = ifelse(frequency_score > 2, "H","L"),
                                                           monetary_type = ifelse(monetary_score > 2, "H", "L"), 
                                                           rfm_type = paste0(recency_type, frequency_type, monetary_type),
                                                           rfm_type = case_when(rfm_type == "HHH" ~ "高價值活躍",
                                                                                rfm_type == "HLH" ~ "重要發展",
                                                                                rfm_type == "LHH" ~ "高價值潛在流失",
                                                                                rfm_type == "LLH" ~ "高價值流失",
                                                                                rfm_type == "HHL" ~ "低價值活躍",
                                                                                rfm_type == "HLL" ~ "一般用戶",
                                                                                rfm_type == "LHL" ~ "低價值潛在流失",
                                                                                rfm_type == "LLL" ~ "低價值流失"))
  next_year <- ap_sale %>% filter(order_day >= as.Date(paste0(order_year-1,"-01-01")) & order_day <= as.Date(paste0(order_year-1,"-12-31"))) %>% 
    select(customer_id, order_id, order_day, total) %>% distinct() %>% group_by(customer_id) %>% 
    summarise(frequency = n_distinct(order_day), total_amount = sum(total))
  purchase_train_data <- purchase_train_rfm %>% left_join(next_year, by = c("customer_id")) %>% mutate(Retain = case_when(frequency > 0 ~ 1, T~0), rfm_type = as.factor(rfm_type)) %>% filter(Retain > 0) %>% 
    select(customer_id, recency_days, transaction_count, amount, rfm_type, total_amount) %>% 
    left_join(ap_mem, by = "customer_id") %>% 
    mutate(rfm_type = as.factor(rfm_type), 
           age = as.factor(age), 
           gender = case_when(gender == 'female' ~ 'female',
                              gender == 'male' ~ 'male',
                              T ~ 'no_gender'),
           gender = as.factor(gender),  
           city =  case_when(city %in% c("臺北市", "新北市", "基隆市", "新竹市", "桃園市", "新竹縣", "宜蘭縣") == T ~ "北部", 
                             city %in% c("臺中市", "苗栗縣", "彰化縣", "南投縣", "雲林縣") == T ~ "中部", 
                             city %in% c("高雄市", "臺南市", "嘉義市", "嘉義縣", "屏東縣", "澎湖縣") == T ~ "南部",
                             city %in% c("花蓮縣", "臺東縣") == T ~ "東部",
                             T ~ "no_city"), 
           city = as.factor(city))
  purchase_train_data1 <- purchase_train_data %>% mutate(log_amount = log(1+amount), log_total_amount = log(total_amount)) %>% select(-customer_id,- join_year, -amount, -total_amount, -join_day)
  null <- lm(log_total_amount ~ 1, purchase_train_data1)
  full <- lm(log_total_amount ~ ., purchase_train_data1)
  model_revenue <- step(null, scope = list(lower = null, upper = full), direction = "forward")
  purchase_rfm_test_data <- ap_sale %>% filter(order_day >= as.Date(paste0(order_year-1,"-01-01")) & order_day <= as.Date(paste0(order_year-1,"-12-31"))) %>% 
    select(customer_id, order_id, order_day, total) %>% distinct() %>% group_by(customer_id) %>%
    summarise(recency = as.numeric(as.Date(paste0(order_year-1,"-12-31"))) - as.numeric(max(order_day)), frequency = n_distinct(order_day), monetary = sum(total)) %>% 
    rfm_table_customer(customer_id = customer_id, n_transactions = frequency, recency_days = recency, total_revenue = monetary, recency_bins = 4, frequency_bins = 4, monetary_bins = 4)
  purchase_test_rfm <- purchase_rfm_test_data$rfm %>% mutate(recency_type = ifelse(recency_score > 2, "H","L"),
                                                             frequency_type = ifelse(frequency_score > 2, "H","L"),
                                                             monetary_type = ifelse(monetary_score > 2, "H", "L"), 
                                                             rfm_type = paste0(recency_type, frequency_type, monetary_type),
                                                             rfm_type = case_when(rfm_type == "HHH" ~ "高價值活躍",
                                                                                  rfm_type == "HLH" ~ "重要發展",
                                                                                  rfm_type == "LHH" ~ "高價值潛在流失",
                                                                                  rfm_type == "LLH" ~ "高價值流失",
                                                                                  rfm_type == "HHL" ~ "低價值活躍",
                                                                                  rfm_type == "HLL" ~ "一般用戶",
                                                                                  rfm_type == "LHL" ~ "低價值潛在流失",
                                                                                  rfm_type == "LLL" ~ "低價值流失"))
  purchase_test_data <- purchase_test_rfm %>% left_join(ap_mem, by = "customer_id") %>% mutate(rfm_type = as.factor(rfm_type), 
                                                                                                                age = as.factor(age), 
                                                                                                                gender = case_when(gender == 'female' ~ 'female',
                                                                                                                                   gender == 'male' ~ 'male',
                                                                                                                                   T ~ 'no_gender'),
                                                                                                                gender = as.factor(gender), 
                                                                                                                city =  case_when(city %in% c("臺北市", "新北市", "基隆市", "新竹市", "桃園市", "新竹縣", "宜蘭縣") == T ~ "北部",
                                                                                                                                  city %in% c("臺中市", "苗栗縣", "彰化縣", "南投縣", "雲林縣") == T ~ "中部",
                                                                                                                                  city %in% c("高雄市", "臺南市", "嘉義市", "嘉義縣", "屏東縣", "澎湖縣") == T ~ "南部",
                                                                                                                                  city %in% c("花蓮縣", "臺東縣") == T ~ "東部",
                                                                                                                                  T ~ "no_city"),
                                                                                                                log_amount = log(1+amount),
                                                                                                                city = as.factor(city)) %>% select(customer_id, recency_days, transaction_count, amount, rfm_type, age, gender, city, log_amount)
  
  purchase_test_data1 <- purchase_test_data %>% mutate(rfm_type = case_when(rfm_type %in% unique(purchase_train_data$rfm_type) == T ~ rfm_type, 
                                                                            str_detect(rfm_type, pattern = "低價值", negate = FALSE) == T ~ "低價值流失",
                                                                            str_detect(rfm_type, pattern = "重要", negate = FALSE) == T ~ "重要發展"),
                                                       city = as.character(city), 
                                                       city = ifelse(city %in% unique(purchase_train_data$city) == T, city, "no_city"), 
                                                       city = as.factor(city))
  pred_revenue <- exp(predict(model_revenue, newdata = purchase_test_data1))
  revenue_result <- cbind(purchase_test_data, pred_revenue)
  return(revenue_result)
}


retain_result <- retain(year(Sys.Date()))
revenue_result <- purchase(year(Sys.Date()))
retain_revenue  <- left_join(retain_result, revenue_result %>% select(customer_id, pred_revenue), by = "customer_id") 

ui <- dashboardPage(
  dashboardHeader(title = "Alpecin有購買商品會員屬性圖", titleWidth = 410),
  dashboardSidebar(
    sidebarMenu(
      menuItem("屬性統計圖", tabName = "plot"),
      menuItem("屬性明細表", tabName = "table"),
      menuItem("購買週期", tabName = "recycle"),
      menuItem("分群_RFM", tabName = "rfm_member_year"),
      menuItem("分群_NAPL", tabName = "napl"),
      menuItem("留存率_RFM", tabName = "retain"),
      menuItem("留存率明細表", tabName = "retain_table")
    )),
  dashboardBody(
    tabItems(
      tabItem(tabName = "plot",
              fluidRow(
                column(12, wellPanel("新會員折線圖", plotlyOutput("order_join_day_plot"))),
                column(6, wellPanel("年齡圓餅圖", plotlyOutput("order_age_pie_plot"))),
                column(6, wellPanel("加入年份圓餅圖", plotlyOutput("order_member_age_pie_plot"))),
                column(6, wellPanel("性別圓餅圖", plotlyOutput("order_gender_pie_plot"))),
                column(6, wellPanel("居住城市圓餅圖", plotlyOutput("order_city_pie_plot")))
              )),
      tabItem(tabName = "table",
              fluidRow(
                column(12, wellPanel("新會員明細表(by年月)", DT::dataTableOutput("order_join_day_table"))),
                column(3, wellPanel("年齡明細表", DT::dataTableOutput("order_age_percent_table"))),
                column(3, wellPanel("加入年份明細表", DT::dataTableOutput("order_member_age_percent_table"))),
                column(3, wellPanel("性別明細表", DT::dataTableOutput("order_gender_percent_table"))),
                column(3, wellPanel("居住城市明細", DT::dataTableOutput("order_city_percent_table"))),
                column(4, wellPanel("年齡 x 加入年分 交叉表", DT::dataTableOutput("order_age_member_age_table"))),
                column(4, wellPanel("年齡 x 性別 交叉表", DT::dataTableOutput("order_age_gender_table"))),
                column(4, wellPanel("加入年分 x 性別", DT::dataTableOutput("order_member_age_gender_table"))),
              )),
      tabItem(tabName = "recycle",
              fluidRow(column(12, wellPanel("回購柱狀圖", plotlyOutput("rebuy_plot")))),
              fluidRow(wellPanel("回購週期",DT::dataTableOutput("repurchase_day")))
      ),
      tabItem(tabName = "rfm_member_year",
              fluidRow(
                column(3, wellPanel(selectInput("select", label = h3("選擇會員加入年分"), choices = c("All", unique(year(ap_sale$join_day)))))),
                column(9, wellPanel("RFM_type柱狀圖", plotlyOutput("rfm_my_plot"))),
                column(6, wellPanel("年齡柱狀圖", plotlyOutput("rfm_my_age_plot"))),
                column(6, wellPanel("性別柱狀圖", plotlyOutput("rfm_my_gender_plot"))),
                column(6, wellPanel("居住城市柱狀圖", plotlyOutput("rfm_my_city_plot"))),
                column(6, wellPanel("加入年份柱狀圖", uiOutput("rfm_my_member_age_plot"))),
              )),
      tabItem(tabName = "napl",
              fluidRow(
                column(3,wellPanel(helpText(HTML("N：在3個購買週期內+購買次數僅1次的會員<br>
                         A：在3個購買週期內+購買次數2次以上的會員，這一群活躍客是最有價值的一群會員<br>
                         P：超過3個購買週期+購買次數2次以上的會員<br>
                         L：超過3個購買週期+僅買1次的會員<br>
                         R：沒有購買過的會員<br>
                         S：有購買過但超過一年沒有再次購買的會員")))),
                column(9, wellPanel(imageOutput("NAPL_image"),style = "overflow: hidden; width: 100%; height: 700px;")),
                column(12, wellPanel("NAPL_type柱狀圖", plotlyOutput("napl_plot"))),
                column(6, wellPanel("年齡柱狀圖", plotlyOutput("napl_age_plot"))),
                column(6, wellPanel("性別柱狀圖", plotlyOutput("napl_gender_plot"))),
                column(6, wellPanel("居住城市柱狀圖", plotlyOutput("napl_city_plot"))),
                column(6, wellPanel("加入年份柱狀圖", plotlyOutput("napl_member_age_plot")))
              )),
      tabItem(tabName = "retain",
              fluidRow(
                column(3, wellPanel(selectInput("select_retain", label = h3("選擇留存率"), choices = c("All", seq(0.1, 1.0, by = 0.1))), helpText(HTML("在2022年有消費過的用戶留存率")))),
                column(9, wellPanel("RFM_type柱狀圖", plotlyOutput("retain_type_plot"))),
                column(6, wellPanel("年齡柱狀圖", plotlyOutput("retain_age_plot"))),
                column(6, wellPanel("性別柱狀圖", plotlyOutput("retain_gender_plot"))),
                column(6, wellPanel("居住城市柱狀圖", plotlyOutput("retain_city_plot")))
              )),
      tabItem(tabName = "retain_table",
              wellPanel("留存率與預估收益明細", DT::dataTableOutput("retain_revenue_table"))) )))

server <- function(input, output) {
  # Plot
  output$order_join_day_plot <- renderPlotly({
    ap_sale %>% group_by(join_day) %>% summarise(n = n_distinct(customer_id)) %>% plot_ly(x = ~join_day, y = ~n, type = "scatter", mode = "line")
  })
  output$order_age_pie_plot <- renderPlotly({
    ap_sale %>% group_by(age) %>% summarise(n = n_distinct(customer_id)) %>% mutate(sum_num = sum(n), prob = round(n/sum_num, 5)*100) %>%
      plot_ly(labels = ~age, values = ~n, type = "pie", textinfo = "percent+label", textposition = "inside")
  })
  output$order_member_age_pie_plot <- renderPlotly({
    ap_sale %>% group_by(join_year) %>% summarise(n = n_distinct(customer_id)) %>% mutate(sum_num = sum(n), prob = paste0(round(n/sum_num, 5)*100,"%")) %>%
      plot_ly(labels = ~join_year, values = ~n, type = "pie", textinfo = "percent+label", textposition = "inside")
  })
  output$order_gender_pie_plot <- renderPlotly({
    ap_sale %>% group_by(gender) %>% summarise(n = n_distinct(customer_id)) %>% mutate(sum_num = sum(n), prob = paste0(round(n/sum_num, 5)*100,"%")) %>%
      plot_ly(labels = ~gender, values = ~n, type = "pie", textinfo = "percent+label", textposition = "inside")
  })
  output$order_city_pie_plot <- renderPlotly({
    ap_sale %>% group_by(city) %>% summarise(n = n_distinct(customer_id)) %>% mutate(sum_num = sum(n), prob = paste0(round(n/sum_num, 5)*100,"%")) %>%
      plot_ly(labels = ~city, values = ~n, type = "pie", textinfo = "percent+label", textposition = "inside")
  })
  
  # Table
  output$order_join_day_table <- DT::renderDataTable(DT::datatable(
    ap_sale %>% group_by(join_year_month = paste(year(join_day), month(join_day), sep = "-")) %>% summarise(n = n_distinct(customer_id)), rownames = F,
    options = list(columnDefs = list(list(className = 'dt-center', targets = '_all')),
                   initComplete = JS(
                     "function(settings, json) {",
                     "$(this.api().table().header()).css({'background-color': '#000', 'color': '#fff'});",
                     "}"))
  ))
  output$order_age_percent_table <- DT::renderDataTable(DT::datatable(
    ap_sale %>% group_by(age) %>% summarise(n = n_distinct(customer_id)) %>% mutate(prob = paste0(round(n/sum(n), 5)*100, "%")), rownames = F,
    options = list(columnDefs = list(list(className = 'dt-center', targets = '_all')),
                   initComplete = JS(
                     "function(settings, json) {",
                     "$(this.api().table().header()).css({'background-color': '#000', 'color': '#fff'});",
                     "}"))
  ))
  output$order_member_age_percent_table <- DT::renderDataTable(DT::datatable(
    ap_sale %>% group_by(join_year) %>% summarise(n = n_distinct(customer_id)) %>% mutate(prob = paste0(round(n/sum(n), 5)*100, "%")), rownames = F,
    options = list(columnDefs = list(list(className = 'dt-center', targets = '_all')),
                   initComplete = JS(
                     "function(settings, json) {",
                     "$(this.api().table().header()).css({'background-color': '#000', 'color': '#fff'});",
                     "}"))
  ))
  output$order_gender_percent_table <- DT::renderDataTable(DT::datatable(
    ap_sale %>% group_by(gender) %>% summarise(n = n_distinct(customer_id)) %>% mutate(prob = paste0(round(n/sum(n), 5)*100, "%")), rownames = F,
    options = list(columnDefs = list(list(className = 'dt-center', targets = '_all')),
                   initComplete = JS(
                     "function(settings, json) {",
                     "$(this.api().table().header()).css({'background-color': '#000', 'color': '#fff'});",
                     "}"))
  ))
  output$order_city_percent_table <- DT::renderDataTable(DT::datatable(
    ap_sale %>% group_by(city) %>% summarise(n = n_distinct(customer_id)) %>% mutate(prob = paste0(round(n/sum(n), 5)*100, "%")), rownames = F,
    options = list(columnDefs = list(list(className = 'dt-center', targets = '_all')),
                   initComplete = JS(
                     "function(settings, json) {",
                     "$(this.api().table().header()).css({'background-color': '#000', 'color': '#fff'});",
                     "}"))
  ))
  output$order_age_member_age_table <- DT::renderDataTable(DT::datatable(
    ap_sale %>% group_by(age, join_year) %>% summarise(n = n_distinct(customer_id)) %>% dcast(age ~ join_year, value.var = "n"), rownames = F,
    options = list(columnDefs = list(list(className = 'dt-center', targets = '_all')),
                   initComplete = JS(
                     "function(settings, json) {",
                     "$(this.api().table().header()).css({'background-color': '#000', 'color': '#fff'});",
                     "}"))
  ))
  output$order_age_gender_table <- DT::renderDataTable(DT::datatable(
    ap_sale %>% group_by(age, gender) %>% summarise(n = n_distinct(customer_id)) %>% dcast(age ~ gender, value.var = "n"), rownames = F,
    options = list(columnDefs = list(list(className = 'dt-center', targets = '_all')),
                   initComplete = JS(
                     "function(settings, json) {",
                     "$(this.api().table().header()).css({'background-color': '#000', 'color': '#fff'});",
                     "}"))
  ))
  output$order_member_age_gender_table <- DT::renderDataTable(DT::datatable(
    ap_sale %>% group_by(join_year, gender) %>% summarise(n = n_distinct(customer_id)) %>% dcast(join_year ~ gender, value.var = "n"), rownames = F,
    options = list(columnDefs = list(list(className = 'dt-center', targets = '_all')),
                   initComplete = JS(
                     "function(settings, json) {",
                     "$(this.api().table().header()).css({'background-color': '#000', 'color': '#fff'});",
                     "}"))
  ))
  colnames(ap_repurchase)
  # recycle
  output$rebuy_plot <- renderPlotly({
    ap_repurchase %>%
      plot_ly(x = ~year, y = ~customer_repurchase_rate,  type = "bar", hoverinfo = "x+y+text", name = "customer", text = ~sprintf("%d%%", round(customer_repurchase_rate * 100,0)), textposition = "inside") %>% 
      add_trace(y = ~ap_shampoo_rp, name = "Alpecin洗髮露", text = ~sprintf("%d%%", round(ap_shampoo_rp * 100,0)), textposition = "inside") %>%
      add_trace(y = ~ap_hair_rp, name = "Alpecin-頭髮液", text = ~sprintf("%d%%", round(ap_hair_rp * 100,0)), textposition = "inside") %>%
      add_trace(y = ~ap_bio_rp, name = "Bioniq 貝歐尼", text = ~sprintf("%d%%", round(ap_bio_rp * 100,0)), textposition = "inside") %>%
      add_trace(y = ~ap_p21_rp, name = "Plantur21", text = ~sprintf("%d%%", round(ap_p21_rp * 100,0)), textposition = "inside") %>%
      add_trace(y = ~ap_p39_shampoo_rp, name = "Plantur39-洗髮露", text = ~sprintf("%d%%", round(ap_p39_shampoo_rp * 100,0)), textposition = "inside") %>%
      add_trace(y = ~ap_p39_hair_rp, name = "Plantur39-頭髮液", text = ~sprintf("%d%%", round(ap_p39_hair_rp * 100,0)), textposition = "inside") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Percentage"))
  })
  output$repurchase_day <- DT::renderDataTable(DT::datatable(
    ap_rp_day, rownames = F, 
    options = list(columnDefs = list(list(className = 'dt-center', targets = '_all')),
                   initComplete = JS(
                     "function(settings, json) {",
                     "$(this.api().table().header()).css({'background-color': '#000', 'color': '#fff'});",
                     "}"))
  ) 
  )
  
  # RFM by member year
  output$value <- renderPrint({ input$select })
  rfm_mem_data <- reactive({
    if(input$select == "All"){
      return(rfm_fun("All"))
    }else{
      return(rfm_fun(input$select))
    }
  })
  output$rfm_my_plot <- renderPlotly({
    rfm_mem_data() %>% group_by(rfm_type) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>% ungroup() %>%
      plot_ly(x = ~rfm_type, y = ~prob,  type = "bar", hoverinfo = "x+y+text", text = ~sprintf("%d%%", round(prob * 100,0)), textposition = "inside") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Prob"))
  })
  output$rfm_my_age_plot <- renderPlotly({
    rfm_mem_data() %>% group_by(rfm_type, age) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>%
      plot_ly(x = ~rfm_type, y = ~prob,  type = "bar", color = ~age, hoverinfo = "x+text", text = ~sprintf("%s：%d%%", age, round(prob * 100, 0)), textposition = "inside") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Prob"))
  })
  output$rfm_my_member_age_plot <- renderUI({
    if (input$select != "All") {
      print("選擇All才會顯示圖形")
    } else {
      plotlyOutput("rfm_age_plot")
    }
  })
  output$rfm_age_plot <- renderPlotly({
    rfm_mem_data() %>% group_by(rfm_type, join_year) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>%
      plot_ly(x = ~rfm_type, y = ~prob,  type = "bar", color = ~as.character(join_year), hoverinfo = "x+text", text = ~sprintf("%s：%d%%", join_year, round(prob * 100, 0)), textposition = "inside") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Prob"))
  })

  output$rfm_my_gender_plot <- renderPlotly({
    rfm_mem_data() %>% mutate(gender = case_when(gender == "female" ~ "女性",
                                                 gender == "male" ~ "男性",
                                                 T ~ "no_gender")) %>% group_by(rfm_type, gender) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>%
      plot_ly(x = ~rfm_type, y = ~prob,  type = "bar", color = ~gender, hoverinfo = "x+text", text = ~sprintf("%s：%d%%", gender, round(prob * 100,0)), textposition = "inside") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Prob"))
  })
  output$rfm_my_city_plot <- renderPlotly({
    rfm_mem_data() %>% mutate(city = case_when(city %in% c("臺北市", "新北市", "基隆市", "新竹市", "桃園市", "新竹縣", "宜蘭縣", "台北市") == T ~ "北部",
                                               city %in% c("臺中市", "台中市", "苗栗縣", "彰化縣", "南投縣", "雲林縣") == T ~ "中部",
                                               city %in% c("高雄市", "臺南市", "台南市", "嘉義市", "嘉義縣", "屏東縣", "澎湖縣") == T ~ "南部",
                                               city %in% c("花蓮縣", "臺東縣", "台東縣") == T ~ "東部",
                                               city %in% c("金門縣", "連江縣") == T ~ "福建省",
                                               T ~ "no_city")) %>%
      group_by(rfm_type, city) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>%
      plot_ly(x = ~rfm_type, y = ~prob,  type = "bar", color = ~city, hoverinfo = "x+text", text = ~sprintf("%s：%d%%", city, round(prob * 100,0)), textposition = "inside") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Prob"))
  })

  # NAPL by order year
  output$NAPL_image <- renderImage({
    list(src = "C:/Users/0344_jason/Desktop/NAPL.png",
         alt = "My Image",
         width = "100%", height = "auto",
         align = "center")
  }, deleteFile = FALSE)
  napl_output <- napl_fun(Sys.Date(), 1) %>% as.data.frame()
  output$napl_plot <- renderPlotly({
    napl_output %>% group_by(napl_type) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>% ungroup() %>%
      plot_ly(x = ~napl_type, y = ~prob,  type = "bar", hoverinfo = "x+y+text", text = ~sprintf("%d%%", round(prob * 100,0)), textposition = "inside") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Prob"))
  })
  output$napl_age_plot <- renderPlotly({
    napl_output %>%
      group_by(napl_type, age) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>%
      plot_ly(x = ~napl_type, y = ~prob,  type = "bar", color = ~age, hoverinfo = "x+text", text = ~sprintf("%s：%d%%", age, round(prob * 100,0)), textposition = "inside") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Prob"))
  })
  output$napl_member_age_plot <- renderPlotly({
    napl_output %>%
      group_by(napl_type, join_year) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>%
      plot_ly(x = ~napl_type, y = ~prob,  type = "bar", color = ~as.character(join_year), hoverinfo = "x+text", text = ~sprintf("%s：%d%%", join_year, round(prob * 100,0)), textposition = "inside") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Prob"))
  })
  output$napl_gender_plot <- renderPlotly({
    napl_output %>% group_by(napl_type, gender) %>% 
      mutate(gender = case_when(gender == "female" ~ "女性", gender == "male" ~ "男性", T ~ "no_gender")) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>%
      plot_ly(x = ~napl_type, y = ~prob,  type = "bar", color = ~gender, hoverinfo = "x+text", text = ~sprintf("%s：%d%%", gender, round(prob * 100,0)), textposition = "inside") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Prob"))
  })
  output$napl_city_plot <- renderPlotly({
    napl_output %>% mutate(city = case_when(city %in% c("臺北市", "台北市", "新北市", "基隆市", "新竹市", "桃園市", "新竹縣", "宜蘭縣") == T ~ "北部",
                                            city %in% c("臺中市", "台中市", "苗栗縣", "彰化縣", "南投縣", "雲林縣") == T ~ "中部",
                                            city %in% c("高雄市", "臺南市", "台南市", "嘉義市", "嘉義縣", "屏東縣", "澎湖縣") == T ~ "南部",
                                            city %in% c("花蓮縣", "臺東縣", "台東縣") == T ~ "東部",
                                            city %in% c("金門縣", "連江縣") == T ~ "福建省",
                                            T ~ "no_city")) %>%
      group_by(napl_type, city) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>%
      plot_ly(x = ~napl_type, y = ~prob,  type = "bar", color = ~city, hoverinfo = "x+text", text = ~sprintf("%s：%d%%", city, round(prob * 100,0)), textposition = "inside") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Prob"))
  })

  # # Retain
  retain_revenue1 <- reactive({
    if(input$select_retain == "All"){
      return(retain_revenue)
    }else{
      return(retain_revenue %>% filter(pred_test >= input$select_retain))
    }
  })
  output$retain_type_plot <- renderPlotly({
    retain_revenue1() %>% group_by(rfm_type) %>% summarise(n = n(), total_revenue = sum(pred_revenue)) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>% ungroup() %>%
      plot_ly(x = ~rfm_type, y = ~prob,  type = "bar", hoverinfo = "x+y+text", text = ~sprintf("%d%%", round(prob * 100,0)), textposition = "inside", name = "RFM_type") %>%
      add_trace(x = ~rfm_type, y = ~total_revenue, type = "scatter", mode = "lines", name = "Revenue", yaxis = "y2") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Prob"), barmode = "stack", yaxis2 = list(overlaying = "y", side = "right"))
  })
  output$retain_age_plot <- renderPlotly({
    retain_revenue1() %>% group_by(rfm_type, age) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>%
      plot_ly(x = ~rfm_type, y = ~prob,  type = "bar", color = ~age, hoverinfo = "x+text", text = ~sprintf("%s：%d%%", age, round(prob * 100, 0)), textposition = "inside") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Prob"))
  })
  output$retain_gender_plot <- renderPlotly({
    retain_revenue1() %>% group_by(rfm_type, gender) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>%
      plot_ly(x = ~rfm_type, y = ~prob,  type = "bar", color = ~gender, hoverinfo = "x+text", text = ~sprintf("%s：%d%%", gender, round(prob * 100,0)), textposition = "inside") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Prob"))
  })
  output$retain_city_plot <- renderPlotly({
    retain_revenue1() %>% group_by(rfm_type, city) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>%
      plot_ly(x = ~rfm_type, y = ~prob,  type = "bar", color = ~city, hoverinfo = "x+text", text = ~sprintf("%s：%d%%", city, round(prob * 100,0)), textposition = "inside") %>%
      layout(yaxis = list(tickformat = ".0%", title = "Prob"))
  })

  # Reatin Table
  output$retain_revenue_table <- DT::renderDataTable(DT::datatable(
    retain_revenue, rownames = F, options = list(pageLength = 100,
                                                 columnDefs = list(list(className = 'dt-center', targets = '_all')),
                                                 initComplete = JS(
                                                   "function(settings, json) {",
                                                   "$(this.api().table().header()).css({'background-color': '#000', 'color': '#fff'});",
                                                   "}"))
  ))
  
}
# Run the app
shinyApp(ui = ui, server = server)



napl_output <- napl_fun(as.Date("2023-08-31"), 1) %>% as.data.frame()
napl_output2 <- napl_fun(as.Date("2023-08-31"), 2) %>% as.data.frame()


napl_output %>% group_by(napl_type) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>% ungroup() %>%
  plot_ly(x = ~napl_type, y = ~prob,  type = "bar", hoverinfo = "x+y+text", text = ~sprintf("%d%%", round(prob * 100,0)), textposition = "outside") %>%
  layout(yaxis = list(tickformat = ".0%", title = "Prob"))



ap_sale %>% select(customer_id, order_id, order_day, total) %>% distinct() %>% group_by(order_day) %>% summarise(sale = sum(total)) %>% plot_ly(x = ~order_day, y = ~sale, type = "scatter", mode = "line")





napl_fun1 <- function(date1, dat){
  # 購買次數(order_day)>1的會員
  napl_id <- ap_sale %>% filter(order_day >= (date1-365) & order_day <= date1) %>% select(customer_id, order_id) %>% distinct() %>% group_by(customer_id) %>% summarise(buy_count = n()) %>% filter(buy_count > 1)
  # 購買次數>1的會員-購買週期
  napl_table2 <- ap_sale %>% filter(order_day >= (date1-365) & order_day <= date1 & customer_id %in% napl_id$customer_id) %>% 
    select(customer_id, order_day) %>% distinct() %>% arrange(customer_id, order_day) %>%
    group_by(customer_id) %>% mutate(purchase_interval = order_day - lag(order_day)) %>% filter(is.na(purchase_interval) == F) %>%
    group_by(customer_id) %>% summarise(purchase_interval = median(purchase_interval))
  # NAPL分類
  napl_table1 <- ap_sale %>% 
    filter(order_day >= (date1-365) & order_day <= date1) %>% 
    select(customer_id, order_id, order_day, total) %>% distinct() %>% group_by(customer_id) %>%
    summarise( recency = as.numeric(date1)-as.numeric(max(order_day)), frequency = n_distinct(order_id), monetary = sum(total)) %>%
    mutate(freq_lable = ifelse(frequency > 1, "multiple", "single"), 
           rece_lable = ifelse(recency < 3*median(napl_table2$purchase_interval), "new", "lost"),
           type = paste(freq_lable, rece_lable, sep = "_"),
           napl_type = case_when(type == "single_new" ~ "N",
                                 type == "single_lost" ~ "L",
                                 type == "multiple_new" ~ "A",
                                 type == "multiple_lost" ~ "P"))
  # 會員屬性
  napl_mem <- ap_mem %>% select(customer_id, age, gender, city, join_year) %>% distinct() %>% select(customer_id, age, gender, city, join_year)
  napl_table1 <- napl_table1 %>% left_join(napl_mem, by = "customer_id")
  # 先前消費過的會員
  pre_sale_mem <- ap_sale %>% filter(order_day <= (date1-365)) %>% select(customer_id, age) %>% distinct()
  # R-註冊後從來沒有消費過的會員
  npal_r_mem <- ap_mem %>% filter(join_day <= date1) %>% select(customer_id, age, gender, city, join_year) %>% mutate(napl_type = "R") %>% 
    filter(!customer_id %in% unique(napl_table1$customer_id) & !customer_id %in% unique(pre_sale_mem$customer_id)) %>% distinct()
  # S-雖然消費過已經超過一年沒有回來買了的會員，是一種「封存」的會員
  npal_s_mem <- ap_mem %>% filter(join_year <= date1) %>% select(customer_id, age, gender, city, join_year) %>% mutate(napl_type = "S") %>% 
    select(customer_id, age, gender, city, join_year, napl_type) %>% filter(!customer_id %in% unique(npal_r_mem$customer_id) & !customer_id %in% unique(napl_table1$customer_id)) %>% distinct()
  npal_rs_mem <- rbind(npal_s_mem, npal_r_mem)
  napl_total <- napl_table1 %>% select(customer_id, age, gender, city, join_year, napl_type) %>% rbind(npal_rs_mem)
  if(dat == 1){
    return(napl_total)
  }else{
    return(napl_table1)
  }
}

date1 <- as.Date("2023-08-31")
napl_id <- ap_sale %>% filter(order_day >= (date1-365) & order_day <= date1) %>% select(customer_id, order_day) %>% distinct() %>% group_by(customer_id) %>% summarise(buy_count = n()) %>% filter(buy_count > 1)
# 購買次數>1的會員-購買週期
napl_table2 <- ap_sale %>% filter(order_day >= (date1-365) & order_day <= date1 & customer_id %in% napl_id$customer_id) %>% 
  select(customer_id, order_day) %>% distinct() %>% arrange(customer_id, order_day) %>%
  group_by(customer_id) %>% mutate(purchase_interval = order_day - lag(order_day)) %>% filter(is.na(purchase_interval) == F) %>%
  group_by(customer_id) %>% summarise(purchase_interval = median(purchase_interval))

median(napl_table2$purchase_interval)




napl_output %>% filter(napl_type == "A") %>% left_join(ap_sale, by = "customer_id") %>% 
  group_by(customer_id) %>% summarise(day_count = n_distinct(order_day), id_count = n_distinct(order_id))

napl_output %>% filter(napl_type == "N") %>% left_join(ap_sale, by = "customer_id") %>% filter(order_day >= (as.Date("2023-08-31")-365)) %>% 
  group_by(customer_id) %>% summarise(day_count = n_distinct(order_day), id_count = n_distinct(order_id)) %>% arrange(desc(id_count))


rfm_output <- rfm_fun("All")
napl_result <- napl_output %>% left_join(ap_mem_data %>% select(customer_id, phone, email), by = "customer_id")
write.csv(napl_output, "C:/Users/0344_jason/Desktop/napl_output.csv", row.names = F, fileEncoding = "big5")
write.csv(napl_result, "C:/Users/0344_jason/Desktop/napl_result.csv", row.names = F, fileEncoding = "big5")
write.csv(new_output1, "C:/Users/0344_jason/Desktop/new_output.csv", row.names = F, fileEncoding = "big5")

new_output1 <- napl_fun1(as.Date("2023-08-31"), 1)
new_output2 <- napl_fun1(as.Date("2023-08-31"), 2)

t2 <- new_output1 %>% left_join(new_output2 %>% select(customer_id, recency, frequency, monetary)) %>% 
  group_by(napl_type) %>% summarise(recency = sum(recency), frequency = sum(frequency), monetary = sum(monetary), counts = n()) %>% 
  mutate(monetary1 = ifelse(is.na(monetary), 0, monetary),total = sum(monetary1), total_counts = sum(counts), sale_prob = monetary1/total, counts_prob = counts/total_counts) %>% 
  data.frame()

write.csv(t2,"C:/Users/0344_jason/Desktop/y2.csv", row.names = F)

ap_mem_data$email

ap_mem_data <- dbGetQuery(msconn,"SELECT * FROM DataTeam.tableau.Alpecin_shopline_customers")



napl_output %>% left_join(napl_output2 %>% select(customer_id, recency, frequency, monetary)) %>% 
  group_by(napl_type) %>% summarise(recency = mean(recency), frequency = mean(frequency), monetary = mean(monetary), counts = n())

retain_revenue1() %>% group_by(rfm_type) %>% summarise(n = n(), total_revenue = sum(pred_revenue)) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>% ungroup() %>%
  plot_ly(x = ~rfm_type, y = ~prob,  type = "bar", hoverinfo = "x+y+text", text = ~sprintf("%d%%", round(prob * 100,0)), textposition = "inside", name = "RFM_type") %>%
  add_trace(x = ~rfm_type, y = ~total_revenue, type = "scatter", mode = "lines", name = "Revenue", yaxis = "y2") %>%
  layout(yaxis = list(tickformat = ".0%", title = "Prob"), barmode = "stack", yaxis2 = list(overlaying = "y", side = "right"))

napl_output %>% left_join(napl_output2 %>% select(customer_id, recency, frequency, monetary)) %>% 
  group_by(napl_type) %>% summarise(recency = sum(recency), frequency = sum(frequency), monetary = sum(monetary), counts = n()) %>% 
  mutate(total = sum(monetary, na.rm = T), total_counts = sum(counts), sale_prob = monetary/total, counts_prob = counts/total_counts) %>%
  plot_ly(x = ~napl_type, y = ~counts_prob,  type = "bar", hoverinfo = "x+y+text", text = ~sprintf("%d%%", round(counts_prob * 100,0)), textposition = "inside", name = "napl_type") %>%
  add_trace(x = ~napl_type, y = ~sale_prob, type = "scatter", mode = "lines", name = "sales", yaxis = "y2") %>%
  layout(yaxis = list(tickformat = ".0%", title = "Prob"), barmode = "stack", yaxis2 = list(overlaying = "y", side = "right"))


napl_output %>% left_join(napl_output2 %>% select(customer_id, recency, frequency, monetary)) %>% 
  group_by(napl_type) %>% summarise(recency = sum(recency), frequency = sum(frequency), monetary = sum(monetary), counts = n()) %>% 
  mutate(total = sum(monetary, na.rm = T), total_counts = sum(counts), sale_prob = monetary/total, counts_prob = counts/total_counts) %>%
  plot_ly(x = ~napl_type, y = ~counts_prob,  type = "bar", hoverinfo = "x+y+text", text = ~sprintf("%d%%", round(counts_prob * 100,0)), textposition = "inside", name = "counts") %>%
  add_trace(x = ~napl_type, y = ~sale_prob, type = "scatter", mode = "lines", name = "sales", text = ~sprintf("%d%%", round(sale_prob * 100,0)), textposition = "inside") %>%
  layout(yaxis = list(tickformat = ".0%", title = "Prob"), font = list(color = "black"))







t1 <- napl_output %>% left_join(napl_output2 %>% select(customer_id, recency, frequency, monetary)) %>% 
  group_by(napl_type) %>% summarise(recency = sum(recency), frequency = sum(frequency), monetary = sum(monetary), counts = n()) %>% 
  mutate(monetary1 = ifelse(is.na(monetary), 0, monetary),total = sum(monetary1), total_counts = sum(counts), sale_prob = monetary1/total, counts_prob = counts/total_counts) %>% 
  data.frame()

write.csv(t1,"C:/Users/0344_jason/Desktop/y1.csv", row.names = F)

ggplot(data = t1) +
  geom_bar(mapping = aes(x = napl_type, y = counts_prob), stat = "identity") +
  geom_line(mapping = aes(x = napl_type, y = sale_prob)) + 
  geom_point(mapping = aes(x = napl_type, y = sale_prob)) 


ggplot(data = t1) +
  geom_line(mapping = aes(x = napl_type, y = sale_prob, group = napl_type)) + 
  geom_point(mapping = aes(x = napl_type, y = sale_prob)) 


ggplot(data = t1, aes(x = napl_type, y = counts_prob)) + 
  geom_bar() +
  geom_smooth(mapping = aes(x = napl_type, y = sale_prob))

napl_output %>% group_by(napl_type) %>% summarise(n = n()) %>% mutate(sum_num = sum(n), prob = n/sum_num) %>% ungroup() %>%
  plot_ly(x = ~napl_type, y = ~prob,  type = "bar", hoverinfo = "x+y+text", text = ~sprintf("%d%%", round(prob * 100,0)), textposition = "inside") %>%
  layout(yaxis = list(tickformat = ".0%", title = "Prob"))



# 新增付款方式、運送方式>>>會員常用、運送時間>>>不同運送方式、訂單成立時段>>>投放廣告
# 付款方式
ap_sale %>% mutate(new_pay_type = case_when(payment_type == "tw_711_b2c_pay" ~ "7_11_pay",
                                            payment_type == "tw_fm_b2c_pay" ~ "fm_pay",
                                            T ~ payment_type)) %>% group_by(order_day, new_pay_type) %>% 
  summarise(n = n_distinct(customer_id)) %>% 
  plot_ly(x = ~order_day, y = ~n, color = ~new_pay_type, type = "scatter", mode = "line")

# 運送方式
ap_sale %>% mutate(deliver_way = case_when(deliver_platform %in% c("tw_711_b2c_pay", "tw_711_b2c_nopay") == T ~ "7_11",
                                           deliver_platform %in% c("tw_fm_b2c_pay", "tw_fm_b2c_nopay") == T ~ "family_mart",
                                           deliver_platform == "custom" ~ "home_delivery",
                                           T ~ deliver_platform)) %>% group_by(order_day, deliver_way) %>% 
  summarise(n = n_distinct(customer_id)) %>% 
  plot_ly(x = ~ order_day, y = ~ n, color = ~ deliver_way, type = "scatter", mode = "line")

# 訂單成立時段
ap_sale %>% filter(order_day >= "2023-07-01" & order_day <= "2023-07-31") %>% mutate(order_hour = hour(created_at)) %>% 
  group_by(order_day, order_hour) %>% summarise(counts = n_distinct(customer_id)) %>%
  arrange(order_day, order_hour) %>% plot_ly(x = ~order_hour, y = ~order_day, z = ~counts, hoverinfo = "x+y+text", text = ~counts, type = 'heatmap', colorscale = 'YlGnBu')




# -------------------------------------------------------------------------------------------------------------------------
ap_sale %>% filter(order_day >= "2023-06-01" & order_day <= "2023-07-31") %>% mutate(order_hour = hour(created_at)) %>% 
  group_by(order_day, order_hour) %>% summarise(counts = n_distinct(customer_id)) %>%
  arrange(order_day, order_hour) %>% dcast(order_day ~ order_hour, value.var = "counts") %>% 
  mutate_all(.,~replace(., is.na(.), 0)) %>% mutate(total = `0`+`1`+`2`+`3`+`4`+`5`+`6`+`7`+`8`+`9`+`10`+`11`+`12`+`13`+`14`+`15`+`16`+`17`+`18`+`19`+`20`+`21`+`22`+`23`, weekday = weekdays(order_day))


rfm_output <- rfm_fun("All") %>% select(-recency_score, -frequency_score, -monetary_score, -rfm_score, -recency_type, -frequency_type, -monetary_type)
napl_output <- napl_fun(Sys.Date(), 1) %>% as.data.frame()
napl_output2 <- napl_fun(Sys.Date(), 2) %>% as.data.frame()


ap_sale %>% filter(order_day >= "2023-06-01" & order_day <= "2023-07-31") %>% 
  left_join(rfm_output %>% select(customer_id, rfm_type), by = "customer_id") %>% 
  filter(str_detect(rfm_type, pattern = "高價值", negate = F) == T | str_detect(rfm_type, pattern = "重要", negate = F) == T) %>%
  mutate(order_hour = hour(created_at)) %>% 
  group_by(order_day, order_hour) %>% summarise(counts = n_distinct(customer_id)) %>%
  arrange(order_day, order_hour) %>% dcast(order_day ~ order_hour, value.var = "counts") %>% 
  mutate_all(.,~replace(., is.na(.), 0)) 


ap_sale %>% filter(order_day >= "2022-08-01" & order_day <= "2023-07-31") %>% 
  left_join(rfm_output %>% select(customer_id, rfm_type), by = "customer_id") %>% 
  mutate(order_hour = hour(created_at)) %>% 
  group_by(order_hour, rfm_type) %>% summarise(counts = n_distinct(customer_id)) %>% 
  plot_ly(x = ~ order_hour, y = ~ counts, color = ~rfm_type, type = "scatter", mode = "line")


ap_sale %>% filter(order_day >= "2023-07-01" & order_day <= "2023-07-31") %>% 
  mutate(order_hour = hour(created_at)) %>% 
  group_by(order_day, order_hour) %>% 
  summarise(counts = n_distinct(customer_id)) %>%
  plot_ly(x = ~order_hour, y = ~order_day, z = ~counts, type = "heatmap") %>%
  layout(annotations = list(x = ~order_hour, y = ~order_day, text = ~counts, showarrow = F, colorscale = "Viridis", font = list(color = c("white", "black")) 
  ))

# rfm購買前幾名商品
ap_sale %>% left_join(rfm_output %>% select(customer_id, rfm_type), by = "customer_id") %>% group_by(rfm_type, QC) %>% summarise(n = sum(quantity)) %>% arrange(rfm_type, desc(n), QC)
# rfm購買的子品牌排名-相對占比
ap_sale %>% left_join(rfm_output %>% select(customer_id, rfm_type), by = "customer_id") %>% 
  group_by(rfm_type, sub_brand) %>% summarise(n = n_distinct(customer_id)) %>% 
  arrange(rfm_type, sub_brand, desc(n)) %>% 
  left_join(rfm_output %>% group_by(rfm_type) %>% summarise(counts = n()), by = "rfm_type") %>%
  mutate(prob = round(n/counts, 3)) %>%
  dcast(sub_brand ~ rfm_type, value.var = "prob") %>%
  mutate_all(.,~replace(., is.na(.), 0)) 
# rm泡泡圖
rfm_output %>% group_by(rfm_type) %>% summarise(recency_days = sum(recency_days), amount = sum(amount), transaction_count = sum(transaction_count), counts = n()) %>% 
  plot_ly(x = ~transaction_count, y = ~amount, text = ~rfm_type, size = ~counts, type = "scatter", mode = "marker", color = ~rfm_type)
# rfm各群平均指標表
rfm_output %>% group_by(rfm_type) %>% summarise(recency_days = mean(recency_days), amount = mean(amount), transaction_count = mean(transaction_count), counts = n()) %>% arrange(desc(counts))

ap_sale %>% left_join(rfm_output %>% select(customer_id, rfm_type), by = "customer_id") %>% group_by(rfm_type, order_day) %>% summarise(n = n_distinct(order_id)) %>% plot_ly(x = ~ order_day, y = ~ n, color = ~ rfm_type, type = "scatter", mode = "line")











rfm_output %>% plot_ly(x = ~rfm_type, y = ~recency_days) %>% add_boxplot(quartilemethod = "linear")
rfm_output %>% plot_ly(x = ~rfm_type, y = ~transaction_count) %>% add_boxplot(quartilemethod = "linear")
rfm_output %>% plot_ly(x = ~rfm_type, y = ~amount) %>% add_boxplot(quartilemethod = "linear")






# 購買次數(order_day)>1的會員
napl_buy_one <- ap_sale %>% filter(order_day >= (Sys.Date()-365) & order_day <= Sys.Date()) %>% select(customer_id, order_day) %>% distinct() %>% group_by(customer_id) %>% summarise(buy_count = n()) %>% filter(buy_count > 1)
# 購買次數>1的會員-購買週期
napl_buy_more <- ap_sale %>% filter(order_day >= (Sys.Date()-365) & order_day <= Sys.Date() & customer_id %in% napl_buy_one$customer_id) %>% 
  select(customer_id, order_day) %>% distinct() %>% arrange(customer_id, order_day) %>%
  group_by(customer_id) %>% mutate(purchase_interval = order_day - lag(order_day)) %>% filter(is.na(purchase_interval) == F) %>%
  group_by(customer_id) %>% summarise(purchase_interval = median(purchase_interval))
median(napl_buy_more$purchase_interval)


napl_output %>% left_join(napl_output2 %>% select(customer_id, recency, frequency, monetary)) %>% 
  group_by(napl_type) %>% summarise(recency = mean(recency), frequency = mean(frequency), monetary = mean(monetary), counts = n())
colnames(napl_output2)

napl_output %>% left_join(ap_sale, by = "customer_id")
napl_output %>% group_by(customer_id) %>% summarise(n = n()) %>% filter(n > 1)
setdiff(napl_output$customer_id, napl_output2$customer_id)












