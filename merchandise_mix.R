library(arules)
library(arulesViz)
library(DBI)
library(reshape2)
library(dplyr)
library(magrittr)
library(stringr)
library(httr)
library(tidyr)
library(keyring)
library(lubridate)
library(RPostgreSQL)
## keyring ##
if(keyring_is_locked()){
  keyring_unlock(password = "12345678")
}
msconn = dbConnect(odbc::odbc(),
                   Driver = "ODBC Driver 17 for SQL SERVER",
                   Server = "10.0.130.225",
                   Database = "DataTeam",
                   UID = "jason_chen",
                   PWD = key_get("db_connect", "jason"),
                   Port = 1433)
drv <- dbDriver("PostgreSQL")
posconn <- dbConnect(drv, user = "tableau", password = "P@SSWORD", dbname = "digital_center", 
                     host = "rf-postgre-rpa.cjxcxnblv1qd.ap-northeast-1.rds.amazonaws.com")
da1 <- paste0("'", paste(year(Sys.Date()-day(Sys.Date())), 
                         ifelse(month(Sys.Date()-day(Sys.Date())) < 10, 
                                paste0("0", month(Sys.Date()-day(Sys.Date()))), 
                                month(Sys.Date()-day(Sys.Date()))), "01", sep = "-"), "'")

# 上月月底
da2 <- paste0("'",Sys.Date()-day(Sys.Date()),"'")
product <- dbGetQuery(msconn, "SELECT DISTINCT parent_brand, QC FROM DataTeam.dbo.ce_product")
brand_mapping <- dbGetQuery(msconn, "SELECT  DISTINCT  istore_parent_brand FROM DataTeam.dbo.parent_brand_mapping WHERE is_cooperation = 1 AND fnc = '買斷'")
merch_un <- data.frame()
merch_mo <- data.frame()
merch_pc <- data.frame()
merch_sp <- data.frame()
merch_ya <- data.frame()
cat('\ncreate aproir fuction: START', as.character.POSIXt(Sys.time()),'\n')
momo <- function(n1, n2, brand){
  m1 <- "SELECT *
FROM(SELECT x.order_id ,
CASE WHEN x.order_id is NULL THEN NULL ELSE 'momo購物' END as display_name,
y.parent_brand,
CASE WHEN b.sub_QC is NULL THEN x.QC ELSE b.sub_QC END as QC_new,
CASE WHEN b.sub_QC is NULL THEN x.tax_price ELSE b.ec_sub_rate*x.tax_price END as new_price
FROM DataTeam.dbo.agent_sale_cost_MOMO x
LEFT JOIN DataTeam.dbo.ce_product y
ON x.QC = y.QC 
LEFT JOIN DataTeam.dbo.ce_product_tetris as b
ON  x.QC = b.QC 
WHERE y.parent_brand IN (SELECT  DISTINCT  istore_parent_brand collate chinese_taiwan_stroke_ci_as
FROM DataTeam.dbo.parent_brand_mapping
WHERE is_cooperation = 1 AND fnc = '買斷') AND x.order_day BETWEEN"
  m2 <- n1
  m3 <- "AND"
  m4 <- n2
  m5 <- "AND x.tax_price > 0 AND x.order_type = '一般訂單') as kk
WHERE kk.new_price >0"
  uni_momo <- dbGetQuery(msconn, paste(m1,m2,m3,m4,m5))
  uni_momo <- uni_momo[uni_momo$parent_brand == brand,]
  # momo購物(因單次購買1件商品對於關聯分析產生規則無太大幫助，故刪除單次購買1件商品交易訂單)
  if(ncol(cbind(uni_momo, new_id = sapply(str_split(uni_momo$order_id, pattern = "-"), "[",1)) %>% 
          select(new_id, QC_new) %>% filter(str_count(new_id, pattern = "#") <1 ) %>% distinct() %>% 
          group_by(new_id) %>% mutate(row = row_number()) %>% 
          spread(row, QC_new) %>% ungroup() %>%
          select(-new_id) %>% as.data.frame()) >1){
    uni_momo1 <- cbind(uni_momo,new_id = sapply(str_split(uni_momo$order_id, pattern = "-"), "[",1)) %>% 
      select(new_id, QC_new) %>% filter(str_count(new_id, pattern = "#") <1 ) %>% distinct() %>% 
      group_by(new_id) %>% mutate(row = row_number()) %>% 
      spread(row, QC_new) %>% ungroup() %>%
      select(-new_id) %>% filter_at(2, all_vars(!is.na(.)))
    # 匯出交易再匯入轉為transactions格式
    write.table(uni_momo1, "/home/rf_0344_jason/data/test/uni_momo_test.csv", sep = ",", na = "", row.names=F, col.names = F)
    um1 <- read.transactions("/home/rf_0344_jason/data/test/uni_momo_test.csv", sep = ',', rm.duplicates = T)
    # 組合商品
    um1_r1 <- apriori(um1, parameter = list(minlen = 2, maxlen = 6, supp = 0.01, conf = 0.7), control = list( verbose = T))
    # 去除冗餘規則
    um1_r1_matrix <- as.matrix(is.subset(x = sort(um1_r1, by = "support"), y = sort(um1_r1, by = "support")))
    # 把這個矩陣的下三角去除，只留上三角的資訊
    um1_r1_matrix[lower.tri(um1_r1_matrix, diag = T)] <- NA
    # 計算每個column中TRUE的個數，若有一個以上的TRUE，代表此column是多餘的
    redundant <- colSums(um1_r1_matrix, na.rm = T) >= 1 
    # 移除多餘的規則
    sort_um1_r1 <- sort(um1_r1, by = "support")
    sort_um1_r1 <- sort_um1_r1[!redundant]
    # 查詢組合共同的母QC(sub_QC查詢母QC)
    k1 <- inspect(sort_um1_r1)
    k1$lhs <- str_replace_all(k1$lhs, pattern = "[[:punct:]]",replacement = "")
    k1$rhs <- str_replace_all(k1$rhs, pattern = "[[:punct:]]",replacement = "")
    k2 <- unique(c(paste0("'",unlist(str_split(k1[,"lhs"], " ")), "'"), paste0("'",unlist(str_split(k1[,"rhs"], " ")), "'")))
    k3 <- k2[1]
    for(i in 2:length(k2)){
      k3 <- paste(k3, k2[i], sep = ",")
    }
    k4 <- dbGetQuery(msconn, paste0(
      "SELECT DISTINCT QC,sub_QC 
FROM DataTeam.dbo.ce_product_tetris
WHERE sub_QC IN (",k3,")")) 
    # 創建組合data.frame
    a1 <- cbind(data.frame(lhs = do.call(rbind, str_split(k1$lhs, " "))) , rhs = k1$rhs)
    # 查詢母QC販售次數 by order_id  
    k4$QC <- paste0("'", k4$QC, "'")
    b1 <- unique(k4$QC)
    b2 <- b1[1]
    for(i in 2:length(b1)){
      b2 <- paste(b2, b1[i], sep = ",")
    }
    # 1個QC對應多個product_id或1個product_id對應多個QC，product_id代表該QC有在該平台上架販售
    b3 <- dbGetQuery(msconn, paste0(
      "SELECT QC,product_id,COUNT(order_id) as buy_count
FROM DataTeam.dbo.agent_sale_cost_MOMO
WHERE QC IN (",b2,") 
GROUP BY QC,product_id"))
    # 可能有1個QC對應多個product_id
    b4 <- b3 %>% group_by(QC) %>% summarise(buy_count = sum(buy_count))
    b5 <- b3 %>% group_by(QC) %>% summarise(id_count = n())
    # 查詢共同母QC
    k1$lhs <- str_replace_all(k1$lhs, pattern = "[[:punct:]]",replacement = "")
    k1$rhs <- str_replace_all(k1$rhs, pattern = "[[:punct:]]",replacement = "")
    if(ncol(a1) == 2){
      k5 <- c() # 共有母QC個數
      k6 <- c() # 共有母QC
      k7 <- c() # lhs與rhs組合，以便比對回原商品組合
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        k5 <- c(k5, length(intersect(t1, t2)))
        k6 <- c(k6, intersect(t1, t2))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], sep = ","), length(intersect(t1, t2))))
      }
    }else if(ncol(a1) == 3){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        k5 <- c(k5, length(intersect(intersect(t1, t2), t3)))
        k6 <- c(k6, intersect(intersect(t1, t2), t3))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], sep = ","), length(intersect(intersect(t1, t2), t3))))
      }
    }else if(ncol(a1) == 4){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(t1, t2), t3), t4)))
        k6 <- c(k6, intersect(intersect(intersect(t1, t2), t3), t4))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], sep = ","), length(intersect(intersect(intersect(t1, t2), t3), t4))))
      }
    }else if(ncol(a1) == 5){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        t5 <- k4[k4$sub_QC == a1[i,5], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5)))
        k6 <- c(k6, intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], a1[i,5], sep = ","), length(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5))))
      }
    }else if(ncol(a1) == 6){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        t5 <- k4[k4$sub_QC == a1[i,5], "QC"]
        t6 <- k4[k4$sub_QC == a1[i,6], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6)))
        k6 <- c(k6, intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], a1[i,5], a1[i,6], sep = ","), length(intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6))))
      }
    }
    # 創建商品組合+母QC，排除母QC為0組合
    m1 <- data.frame(mix_com = k7, p_qc = k6) 
    m1$p_qc <- str_replace_all(m1$p_qc, pattern = "[[:punct:]]", replacement = "") %>% as.numeric()
    # 母QC是否有momo的product_id
    m2 <- left_join(m1, b5, by = c("p_qc" = "QC")) %>% group_by(mix_com) %>% summarise(pqc_id = ifelse(sum(complete.cases(id_count)) > 0, 1, 0))
    # 母QC是否在momo販售
    m3 <- left_join(m1, b4, by = c("p_qc" = "QC")) %>% group_by(mix_com) %>% summarise(pqc_buy = ifelse(sum(complete.cases(buy_count)) > 0, 1, 0))
    # 資料合併
    a1 <- cbind(a1, com_pqc_count = k5, mix_com = apply(a1, 1, function(a) str_c(a, collapse = ","))) %>% left_join(m2, by = "mix_com") %>% left_join(m3, by = "mix_com")
    # 篩選選需欄位(無共同母QC或無product_id及無購買紀錄)
    a2 <- data.frame(lhs = k1$lhs, rhs = k1$rhs, a1[,c("com_pqc_count", "pqc_id", "pqc_buy")], 
                     order_count = k1$count, support = k1$support, confidence = k1$confidence, lift = k1$lift) %>% 
      filter(com_pqc_count == 0 | pqc_id == 0 & pqc_buy == 0) %>% 
      select(-c(pqc_id, pqc_buy))
    a2$parent_brand <- rep(brand, nrow(a2))
    return(a2)
  }else{
    return(NULL)
  }
  
}
pchome <- function(n1, n2, brand){
  uni_pchome <- dbGetQuery(posconn, paste0(paste0("SELECT  order_id, platform_name ,",paste0('"',"sub_QC",'"'), "AS QC_new
FROM sales.completed_orders
WHERE real_sale_price > 0 
AND platform_name = 'PChome購物'
AND order_day BETWEEN "),n1,"AND",n2)) %>% 
    left_join(product %>% mutate(QC = as.character(QC)), by = c("qc_new" = "QC")) %>% 
    rename("QC_new"="qc_new") %>% filter(parent_brand %in% brand_mapping$istore_parent_brand)
  uni_pchome <- uni_pchome[uni_pchome$parent_brand == brand,]
  if(ncol(uni_pchome %>% select(order_id, QC_new) %>% distinct() %>%
          group_by(order_id) %>% mutate(row = row_number()) %>% 
          spread(row, QC_new) %>% ungroup() %>% 
          select(-order_id) %>% as.data.frame()) > 1){
    uni_pchome1 <- uni_pchome %>% select(order_id, QC_new) %>% distinct() %>%
      group_by(order_id) %>% mutate(row = row_number()) %>% 
      spread(row, QC_new) %>% ungroup() %>% 
      select(-order_id) %>% filter_at(2, all_vars(!is.na(.)))
    # 匯出交易再匯入轉為transactions格式
    write.table(uni_pchome1, "/home/rf_0344_jason/data/test/uni_pchome_test.csv", sep = ",", na = "", row.names=FALSE, col.names = FALSE)
    up1 <- read.transactions("/home/rf_0344_jason/data/test/uni_pchome_test.csv", sep = ',', rm.duplicates = T)
    summary(up1)
    # 建立規則門檻值：supp = 0.01, conf = 0.01
    up1_r1 <- apriori(up1, parameter = list(minlen = 2, maxlen = 6, supp = 0.01, conf = 0.7), control = list( verbose = T))
    # 去除冗餘規則
    up1_r1_matrix <- as.matrix(is.subset(x = sort(up1_r1, by = "support"), y = sort(up1_r1, by = "support"))) # 建立矩陣
    up1_r1_matrix[lower.tri(up1_r1_matrix, diag = T)] <- NA # 把這個矩陣的下三角去除，只留上三角的資訊
    redundant <- colSums(up1_r1_matrix, na.rm = T) >= 1 # 計算每個column中TRUE的個數，若有一個以上的TRUE，代表此column是多餘的
    # 移除多餘的規則
    sort_up1_r1 <- sort(up1_r1, by = "support")
    sort_up1_r1 <- sort_up1_r1[!redundant]
    # 查詢組合共同的母QC(sub_QC查詢母QC)
    k1 <- inspect(sort_up1_r1)
    k1$lhs <- str_replace_all(k1$lhs, pattern = "[[:punct:]]",replacement = "")
    k1$rhs <- str_replace_all(k1$rhs, pattern = "[[:punct:]]",replacement = "")
    k2 <- unique(c(paste0("'",unlist(str_split(k1[,"lhs"], " ")), "'"), paste0("'",unlist(str_split(k1[,"rhs"], " ")), "'")))
    k3 <- k2[1]
    for(i in 2:length(k2)){
      k3 <- paste(k3, k2[i], sep = ",")
    }
    k4 <- dbGetQuery(msconn, paste0(
      "SELECT DISTINCT QC,sub_QC 
FROM DataTeam.dbo.ce_product_tetris
WHERE sub_QC IN (",k3,")")) 
    # 創建組合data.frame
    a1 <- cbind(data.frame(lhs = do.call(rbind, str_split(k1$lhs, " "))) , rhs = k1$rhs)
    # 查詢母QC販售次數 by order_id  
    k4$QC <- paste0("'", k4$QC, "'")
    b1 <- unique(k4$QC)
    b2 <- b1[1]
    for(i in 2:length(b1)){
      b2 <- paste(b2, b1[i], sep = ",")
    }
    b3 <- dbGetQuery(posconn, paste0(
      "SELECT" ,paste0('"',"QC",'"'),", COUNT(order_id) as buy_count
    FROM sales.completed_orders
    WHERE ",paste0('"',"QC",'"'), "IN (",b2,") AND platform_name = 'PChome購物'
    GROUP BY ",paste0('"',"QC",'"')))
    # 可能有1個QC對應多個product_id
    b4 <- b3 %>% group_by(QC) %>% summarise(buy_count = sum(buy_count))
    # 查詢共同母QC
    k1$lhs <- str_replace_all(k1$lhs, pattern = "[[:punct:]]",replacement = "")
    k1$rhs <- str_replace_all(k1$rhs, pattern = "[[:punct:]]",replacement = "")
    if(ncol(a1) == 2){
      k5 <- c() # 共有母QC個數
      k6 <- c() # 共有母QC
      k7 <- c() # lhs與rhs組合，以便比對回原商品組合
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        k5 <- c(k5, length(intersect(t1, t2)))
        k6 <- c(k6, intersect(t1, t2))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], sep = ","), length(intersect(t1, t2))))
      }
    }else if(ncol(a1) == 3){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        k5 <- c(k5, length(intersect(intersect(t1, t2), t3)))
        k6 <- c(k6, intersect(intersect(t1, t2), t3))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], sep = ","), length(intersect(intersect(t1, t2), t3))))
      }
    }else if(ncol(a1) == 4){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(t1, t2), t3), t4)))
        k6 <- c(k6, intersect(intersect(intersect(t1, t2), t3), t4))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], sep = ","), length(intersect(intersect(intersect(t1, t2), t3), t4))))
      }
    }else if(ncol(a1) == 5){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        t5 <- k4[k4$sub_QC == a1[i,5], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5)))
        k6 <- c(k6, intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], a1[i,5], sep = ","), length(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5))))
      }
    }else if(ncol(a1) == 6){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        t5 <- k4[k4$sub_QC == a1[i,5], "QC"]
        t6 <- k4[k4$sub_QC == a1[i,6], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6)))
        k6 <- c(k6, intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], a1[i,5], a1[i,6], sep = ","), length(intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6))))
      }
    }
    # 創建商品組合+母QC，排除母QC為0組合
    m1 <- data.frame(mix_com = k7, p_qc = k6) 
    m1$p_qc <- str_replace_all(m1$p_qc, pattern = "[[:punct:]]", replacement = "") %>% as.numeric()
    # 母QC是否在pchome販售
    m2 <- left_join(m1 %>% mutate(p_qc = as.character(p_qc)), b4, by = c("p_qc" = "QC")) %>% group_by(mix_com) %>% summarise(pqc_buy = ifelse(sum(complete.cases(buy_count)) > 0, 1, 0))
    # 資料合併
    a1 <- cbind(a1, com_pqc_count = k5, mix_com = apply(a1, 1, function(a) str_c(a, collapse = ","))) %>% left_join(m2, by = "mix_com")
    # 篩選選需欄位(無共同母QC或無購買紀錄)
    a2 <- data.frame(lhs = k1$lhs, rhs = k1$rhs, a1[,c("com_pqc_count", "pqc_buy")], 
                     order_count = k1$count, support = k1$support, confidence = k1$confidence, lift = k1$lift) %>% 
      filter(com_pqc_count == 0 | pqc_buy == 0) %>% 
      select(-c(pqc_buy))
    a2$parent_brand <- rep(brand, nrow(a2))
    return(a2)
  }else{
    return(NULL)
  }
  
}
shopee <- function(n1, n2, brand){
  uni_shopee <- dbGetQuery(posconn, paste0(paste0("SELECT  order_id, platform_name ,",paste0('"',"sub_QC",'"'), "AS QC_new
FROM sales.completed_orders
WHERE real_sale_price > 0 
AND platform_name = '蝦皮購物'
AND order_day BETWEEN "),n1,"AND",n2)) %>% 
    left_join(product %>% mutate(QC = as.character(QC)), by = c("qc_new" = "QC")) %>% 
    rename("QC_new"="qc_new") %>% filter(parent_brand %in% brand_mapping$istore_parent_brand)
  uni_shopee <- uni_shopee[uni_shopee$parent_brand == brand,]
  if(ncol(uni_shopee %>% select(order_id, QC_new) %>% distinct() %>%
          group_by(order_id) %>% mutate(row = row_number()) %>% 
          spread(row, QC_new) %>% ungroup() %>% 
          select(-order_id) %>% as.data.frame()) > 1){
    # shopee(因單次購買1件商品對於關聯分析產生規則無太大幫助，故刪除單次購買1件商品交易訂單)
    uni_shopee1 <- uni_shopee %>% select(order_id, QC_new) %>% distinct() %>%
      group_by(order_id) %>% mutate(row = row_number()) %>% 
      spread(row, QC_new) %>% ungroup() %>% 
      select(-order_id) %>% filter_at(2, all_vars(!is.na(.)))
    # 匯出交易再匯入轉為transactions格式
    write.table(uni_shopee1, "/home/rf_0344_jason/data/test/uni_shopee_test.csv", sep = ",", na = "", row.names=FALSE, col.names = FALSE)
    us1 <- read.transactions("/home/rf_0344_jason/data/test/uni_shopee_test.csv", sep = ',', rm.duplicates = T)
    # 建立規則門檻值：supp = 0.01, conf = 0.01
    us1_r1 <- apriori(us1, parameter = list(minlen = 2, maxlen = 6, supp = 0.01, conf = 0.7), control = list( verbose = T))
    # 去除冗餘規則
    us1_r1_matrix <- as.matrix(is.subset(x = sort(us1_r1, by = "support"), y = sort(us1_r1, by = "support"))) # 建立矩陣
    us1_r1_matrix[lower.tri(us1_r1_matrix, diag = T)] <- NA # 把這個矩陣的下三角去除，只留上三角的資訊
    redundant <- colSums(us1_r1_matrix, na.rm = T) >= 1 # 計算每個column中TRUE的個數，若有一個以上的TRUE，代表此column是多餘的
    # 移除多餘的規則
    sort_us1_r1 <- sort(us1_r1, by = "support")
    sort_us1_r1 <- sort_us1_r1[!redundant]
    # 查詢組合共同的母QC(sub_QC查詢母QC)
    k1 <- inspect(sort_us1_r1)
    k1$lhs <- str_replace_all(k1$lhs, pattern = "[[:punct:]]",replacement = "")
    k1$rhs <- str_replace_all(k1$rhs, pattern = "[[:punct:]]",replacement = "")
    k2 <- unique(c(paste0("'",unlist(str_split(k1[,"lhs"], " ")), "'"), paste0("'",unlist(str_split(k1[,"rhs"], " ")), "'")))
    k3 <- k2[1]
    for(i in 2:length(k2)){
      k3 <- paste(k3, k2[i], sep = ",")
    }
    k4 <- dbGetQuery(msconn, paste0(
      "SELECT DISTINCT QC,sub_QC 
FROM DataTeam.dbo.ce_product_tetris
WHERE sub_QC IN (",k3,")")) 
    # 創建組合data.frame
    a1 <- cbind(data.frame(lhs = do.call(rbind, str_split(k1$lhs, " "))) , rhs = k1$rhs)
    # 查詢母QC販售次數 by order_id  
    k4$QC <- paste0("'", k4$QC, "'")
    b1 <- unique(k4$QC)
    b2 <- b1[1]
    for(i in 2:length(b1)){
      b2 <- paste(b2, b1[i], sep = ",")
    }
    b3 <- dbGetQuery(posconn, paste0(
      "SELECT" ,paste0('"',"QC",'"'),", COUNT(order_id) as buy_count
    FROM sales.completed_orders
    WHERE ",paste0('"',"QC",'"'), "IN (",b2,") AND platform_name = '蝦皮購物'
    GROUP BY ",paste0('"',"QC",'"')))
    # 可能有1個QC對應多個product_id
    b4 <- b3 %>% group_by(QC) %>% summarise(buy_count = sum(buy_count))
    # 查詢共同母QC
    k1$lhs <- str_replace_all(k1$lhs, pattern = "[[:punct:]]",replacement = "")
    k1$rhs <- str_replace_all(k1$rhs, pattern = "[[:punct:]]",replacement = "")
    if(ncol(a1) == 2){
      k5 <- c() # 共有母QC個數
      k6 <- c() # 共有母QC
      k7 <- c() # lhs與rhs組合，以便比對回原商品組合
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        k5 <- c(k5, length(intersect(t1, t2)))
        k6 <- c(k6, intersect(t1, t2))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], sep = ","), length(intersect(t1, t2))))
      }
    }else if(ncol(a1) == 3){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        k5 <- c(k5, length(intersect(intersect(t1, t2), t3)))
        k6 <- c(k6, intersect(intersect(t1, t2), t3))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], sep = ","), length(intersect(intersect(t1, t2), t3))))
      }
    }else if(ncol(a1) == 4){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(t1, t2), t3), t4)))
        k6 <- c(k6, intersect(intersect(intersect(t1, t2), t3), t4))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], sep = ","), length(intersect(intersect(intersect(t1, t2), t3), t4))))
      }
    }else if(ncol(a1) == 5){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        t5 <- k4[k4$sub_QC == a1[i,5], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5)))
        k6 <- c(k6, intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], a1[i,5], sep = ","), length(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5))))
      }
    }else if(ncol(a1) == 6){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        t5 <- k4[k4$sub_QC == a1[i,5], "QC"]
        t6 <- k4[k4$sub_QC == a1[i,6], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6)))
        k6 <- c(k6, intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], a1[i,5], a1[i,6], sep = ","), length(intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6))))
      }
    }
    # 創建商品組合+母QC，排除母QC為0組合
    m1 <- data.frame(mix_com = k7, p_qc = k6) 
    m1$p_qc <- str_replace_all(m1$p_qc, pattern = "[[:punct:]]", replacement = "") %>% as.numeric()
    # 母QC是否在pchome販售
    m2 <- left_join(m1 %>% mutate(p_qc = as.character(p_qc)), b4, by = c("p_qc" = "QC")) %>% group_by(mix_com) %>% summarise(pqc_buy = ifelse(sum(complete.cases(buy_count)) > 0, 1, 0))
    # 資料合併
    a1 <- cbind(a1, com_pqc_count = k5, mix_com = apply(a1, 1, function(a) str_c(a, collapse = ","))) %>% left_join(m2, by = "mix_com")
    # 篩選選需欄位(無共同母QC或無購買紀錄)
    a2 <- data.frame(lhs = k1$lhs, rhs = k1$rhs, a1[,c("com_pqc_count", "pqc_buy")], 
                     order_count = k1$count, support = k1$support, confidence = k1$confidence, lift = k1$lift) %>% 
      filter(com_pqc_count == 0 | pqc_buy == 0) %>% 
      select(-c(pqc_buy))
    a2$parent_brand <- rep(brand, nrow(a2))
    return(a2)
  }else{
    return(NULL)
  }
}
yahoo <- function(n1, n2, brand){
  uni_yahoo <- dbGetQuery(posconn, paste0(paste0("SELECT  order_id, platform_name ,",paste0('"',"sub_QC",'"'), "AS QC_new
FROM sales.completed_orders
WHERE real_sale_price > 0 
AND platform_name = 'Yahoo購物'
AND order_day BETWEEN "),n1,"AND",n2)) %>% 
    left_join(product %>% mutate(QC = as.character(QC)), by = c("qc_new" = "QC")) %>% 
    rename("QC_new"="qc_new") %>% filter(parent_brand %in% brand_mapping$istore_parent_brand)
  uni_yahoo <- uni_yahoo[uni_yahoo$parent_brand == brand,]
  if(ncol(uni_yahoo %>% select(order_id, QC_new) %>% distinct() %>%
          group_by(order_id) %>% mutate(row = row_number()) %>% 
          spread(row, QC_new) %>% ungroup() %>% 
          select(-order_id) %>% as.data.frame()) > 1){
    # Yahoo購物
    uni_yahoo1 <- uni_yahoo %>% select(order_id, QC_new) %>% distinct() %>%
      group_by(order_id) %>% mutate(row = row_number()) %>% 
      spread(row, QC_new) %>% ungroup() %>% 
      select(-order_id) %>% filter_at(2, all_vars(!is.na(.)))
    # 匯出交易再匯入轉為transactions格式
    write.table(uni_yahoo1, "/home/rf_0344_jason/data/test/uni_yahoo_test.csv", sep = ",", na = "", row.names=FALSE, col.names = FALSE)
    uy1 <- read.transactions("/home/rf_0344_jason/data/test/uni_yahoo_test.csv", sep = ',', rm.duplicates = T)
    # 建立規則門檻值：supp = 0.01, conf = 0.01
    uy1_r1 <- apriori(uy1, parameter = list(minlen = 2, maxlen = 6, supp = 0.01, conf = 0.7), control = list( verbose = T))
    # 去除冗餘規則
    uy1_r1_matrix <- as.matrix(is.subset(x = sort(uy1_r1, by = "support"), y = sort(uy1_r1, by = "support"))) # 建立矩陣
    uy1_r1_matrix[lower.tri(uy1_r1_matrix, diag = T)] <- NA # 把這個矩陣的下三角去除，只留上三角的資訊
    redundant <- colSums(uy1_r1_matrix, na.rm = T) >= 1 # 計算每個column中TRUE的個數，若有一個以上的TRUE，代表此column是多餘的
    # 移除多餘的規則
    sort_uy1_r1 <- sort(uy1_r1, by = "support")
    sort_uy1_r1 <- sort_uy1_r1[!redundant]
    # 查詢組合共同的母QC(sub_QC查詢母QC)
    k1 <- inspect(sort_uy1_r1)
    k1$lhs <- str_replace_all(k1$lhs, pattern = "[[:punct:]]",replacement = "")
    k1$rhs <- str_replace_all(k1$rhs, pattern = "[[:punct:]]",replacement = "")
    k2 <- unique(c(paste0("'",unlist(str_split(k1[,"lhs"], " ")), "'"), paste0("'",unlist(str_split(k1[,"rhs"], " ")), "'")))
    k3 <- k2[1]
    for(i in 2:length(k2)){
      k3 <- paste(k3, k2[i], sep = ",")
    }
    k4 <- dbGetQuery(msconn, paste0(
      "SELECT DISTINCT QC,sub_QC 
FROM DataTeam.dbo.ce_product_tetris
WHERE sub_QC IN (",k3,")")) 
    # 創建組合data.frame
    a1 <- cbind(data.frame(lhs = do.call(rbind, str_split(k1$lhs, " "))) , rhs = k1$rhs)
    # 查詢母QC販售次數 by order_id  
    k4$QC <- paste0("'", k4$QC, "'")
    b1 <- unique(k4$QC)
    b2 <- b1[1]
    for(i in 2:length(b1)){
      b2 <- paste(b2, b1[i], sep = ",")
    }
    b3 <- dbGetQuery(posconn, paste0(
      "SELECT" ,paste0('"',"QC",'"'),", COUNT(order_id) as buy_count
    FROM sales.completed_orders
    WHERE ",paste0('"',"QC",'"'), "IN (",b2,") AND platform_name = 'Yahoo購物'
    GROUP BY ",paste0('"',"QC",'"')))
    # 可能有1個QC對應多個product_id
    b4 <- b3 %>% group_by(QC) %>% summarise(buy_count = sum(buy_count))
    # 查詢共同母QC
    k1$lhs <- str_replace_all(k1$lhs, pattern = "[[:punct:]]",replacement = "")
    k1$rhs <- str_replace_all(k1$rhs, pattern = "[[:punct:]]",replacement = "")
    if(ncol(a1) == 2){
      k5 <- c() # 共有母QC個數
      k6 <- c() # 共有母QC
      k7 <- c() # lhs與rhs組合，以便比對回原商品組合
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        k5 <- c(k5, length(intersect(t1, t2)))
        k6 <- c(k6, intersect(t1, t2))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], sep = ","), length(intersect(t1, t2))))
      }
    }else if(ncol(a1) == 3){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        k5 <- c(k5, length(intersect(intersect(t1, t2), t3)))
        k6 <- c(k6, intersect(intersect(t1, t2), t3))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], sep = ","), length(intersect(intersect(t1, t2), t3))))
      }
    }else if(ncol(a1) == 4){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(t1, t2), t3), t4)))
        k6 <- c(k6, intersect(intersect(intersect(t1, t2), t3), t4))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], sep = ","), length(intersect(intersect(intersect(t1, t2), t3), t4))))
      }
    }else if(ncol(a1) == 5){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        t5 <- k4[k4$sub_QC == a1[i,5], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5)))
        k6 <- c(k6, intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], a1[i,5], sep = ","), length(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5))))
      }
    }else if(ncol(a1) == 6){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        t5 <- k4[k4$sub_QC == a1[i,5], "QC"]
        t6 <- k4[k4$sub_QC == a1[i,6], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6)))
        k6 <- c(k6, intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], a1[i,5], a1[i,6], sep = ","), length(intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6))))
      }
    }
    # 創建商品組合+母QC，排除母QC為0組合
    m1 <- data.frame(mix_com = k7, p_qc = k6) 
    m1$p_qc <- str_replace_all(m1$p_qc, pattern = "[[:punct:]]", replacement = "") %>% as.numeric()
    # 母QC是否在pchome販售
    m2 <- left_join(m1 %>% mutate(p_qc = as.character(p_qc)), b4, by = c("p_qc" = "QC")) %>% group_by(mix_com) %>% summarise(pqc_buy = ifelse(sum(complete.cases(buy_count)) > 0, 1, 0))
    # 資料合併
    a1 <- cbind(a1, com_pqc_count = k5, mix_com = apply(a1, 1, function(a) str_c(a, collapse = ","))) %>% left_join(m2, by = "mix_com")
    # 篩選選需欄位(無共同母QC或無購買紀錄)
    a2 <- data.frame(lhs = k1$lhs, rhs = k1$rhs, a1[,c("com_pqc_count", "pqc_buy")], 
                     order_count = k1$count, support = k1$support, confidence = k1$confidence, lift = k1$lift) %>% 
      filter(com_pqc_count == 0 | pqc_buy == 0) %>% 
      select(-c(pqc_buy))
    a2$parent_brand <- rep(brand, nrow(a2))
    return(a2)
  }else{
    return(NULL)
  }
}
undisplay <- function(n1, n2, brand){
  uni_com <- dbGetQuery(posconn, paste0(paste0("SELECT order_id, platform_name ,",paste0('"',"sub_QC",'"'), "AS QC_new
FROM sales.completed_orders
WHERE real_sale_price > 0 
AND platform_name NOT IN ('momo購物')
AND order_day BETWEEN "),n1,"AND",n2,paste0("UNION SELECT SUBSTRING(order_id,1,14) as order_id, platform_name ,",paste0('"',"sub_QC",'"'), "AS QC_new
FROM sales.completed_orders
WHERE real_sale_price > 0 
AND platform_name IN ('momo購物')
AND order_day BETWEEN "),n1,"AND",n2)) %>% 
    left_join(product %>% mutate(QC = as.character(QC)), by = c("qc_new" = "QC")) %>% 
    rename("QC_new"="qc_new") %>% filter(parent_brand == brand) %>% select(order_id, QC_new) %>% distinct()
  if(ncol(uni_com %>% group_by(order_id) %>% mutate(row = row_number()) %>% 
          spread(row, QC_new) %>% ungroup() %>% 
          select(-order_id) %>% as.data.frame()) > 1){
    # 因單次購買1件商品對於關聯分析產生規則無太大幫助，故刪除單次購買1件商品交易訂單
    uni_com <- uni_com %>% group_by(order_id) %>% mutate(row = row_number()) %>% 
      spread(row, QC_new) %>% ungroup() %>% 
      select(-order_id) %>% filter_at(2, all_vars(!is.na(.)))
    # 匯出交易再匯入轉為transactions格式
    write.table(uni_com, "/home/rf_0344_jason/data/test/uni_com_test.csv", sep = ",", na = "", row.names=FALSE, col.names = FALSE)
    ucom1 <- read.transactions("/home/rf_0344_jason/data/test/uni_com_test.csv", sep = ',', rm.duplicates = T)
    # 建立規則門檻值：supp = 0.001, conf = 0.05
    ucom1_r1 <- apriori(ucom1, parameter = list(minlen = 2, maxlen = 6, supp = 0.001, conf = 0.7), control = list( verbose = T))
    ucom1_r1 <- subset(ucom1_r1, subset = count >= 5)
    # 去除冗餘規則
    ucom1_r1_matrix <- as.matrix(is.subset(x = sort(ucom1_r1, by = "support"), y = sort(ucom1_r1, by = "support"))) # 建立矩陣
    ucom1_r1_matrix[lower.tri(ucom1_r1_matrix, diag = T)] <- NA # 把這個矩陣的下三角去除，只留上三角的資訊
    redundant <- colSums(ucom1_r1_matrix, na.rm = T) >= 1 # 計算每個column中TRUE的個數，若有一個以上的TRUE，代表此column是多餘的
    # 移除多餘的規則
    sort_ucom1_r1 <- sort(ucom1_r1, by = "support")
    sort_ucom1_r1 <- sort_ucom1_r1[!redundant]
    # 查詢組合共同的母QC(sub_QC查詢母QC)
    k1 <- inspect(sort_ucom1_r1)
    k1$lhs <- str_replace_all(k1$lhs, pattern = "[[:punct:]]",replacement = "")
    k1$rhs <- str_replace_all(k1$rhs, pattern = "[[:punct:]]",replacement = "")
    k2 <- unique(c(paste0("'",unlist(str_split(k1[,"lhs"], " ")), "'"), paste0("'",unlist(str_split(k1[,"rhs"], " ")), "'")))
    k3 <- k2[1]
    for(i in 2:length(k2)){
      k3 <- paste(k3, k2[i], sep = ",")
    }
    k4 <- dbGetQuery(msconn, paste0(
      "SELECT DISTINCT QC,sub_QC 
FROM DataTeam.dbo.ce_product_tetris
WHERE sub_QC IN (",k3,")")) 
    # 創建組合data.frame
    a1 <- cbind(data.frame(lhs = do.call(rbind, str_split(k1$lhs, " "))) , rhs = k1$rhs)
    # 查詢母QC販售次數 by order_id  
    k4$QC <- paste0("'", k4$QC, "'")
    b1 <- unique(k4$QC)
    b2 <- b1[1]
    for(i in 2:length(b1)){
      b2 <- paste(b2, b1[i], sep = ",")
    }
    b3 <- dbGetQuery(posconn, paste0(
      "SELECT" ,paste0('"',"QC",'"'),", COUNT(order_id) as buy_count
    FROM sales.completed_orders
    WHERE ",paste0('"',"QC",'"'), "IN (",b2,")
    GROUP BY ",paste0('"',"QC",'"')))
    # 可能有1個QC對應多個product_id
    b4 <- b3 %>% group_by(QC) %>% summarise(buy_count = sum(buy_count))
    # 查詢共同母QC
    k1$lhs <- str_replace_all(k1$lhs, pattern = "[[:punct:]]",replacement = "")
    k1$rhs <- str_replace_all(k1$rhs, pattern = "[[:punct:]]",replacement = "")
    if(ncol(a1) == 2){
      k5 <- c() # 共有母QC個數
      k6 <- c() # 共有母QC
      k7 <- c() # lhs與rhs組合，以便比對回原商品組合
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        k5 <- c(k5, length(intersect(t1, t2)))
        k6 <- c(k6, intersect(t1, t2))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], sep = ","), length(intersect(t1, t2))))
      }
    }else if(ncol(a1) == 3){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        k5 <- c(k5, length(intersect(intersect(t1, t2), t3)))
        k6 <- c(k6, intersect(intersect(t1, t2), t3))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], sep = ","), length(intersect(intersect(t1, t2), t3))))
      }
    }else if(ncol(a1) == 4){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(t1, t2), t3), t4)))
        k6 <- c(k6, intersect(intersect(intersect(t1, t2), t3), t4))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], sep = ","), length(intersect(intersect(intersect(t1, t2), t3), t4))))
      }
    }else if(ncol(a1) == 5){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        t5 <- k4[k4$sub_QC == a1[i,5], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5)))
        k6 <- c(k6, intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], a1[i,5], sep = ","), length(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5))))
      }
    }else if(ncol(a1) == 6){
      k5 <- c()
      k6 <- c()
      k7 <- c()
      for(i in 1:nrow(k1)){
        t1 <- k4[k4$sub_QC == a1[i,1], "QC"]
        t2 <- k4[k4$sub_QC == a1[i,2], "QC"]
        t3 <- k4[k4$sub_QC == a1[i,3], "QC"]
        t4 <- k4[k4$sub_QC == a1[i,4], "QC"]
        t5 <- k4[k4$sub_QC == a1[i,5], "QC"]
        t6 <- k4[k4$sub_QC == a1[i,6], "QC"]
        k5 <- c(k5, length(intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6)))
        k6 <- c(k6, intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6))
        k7 <- c(k7, rep(paste(a1[i,1], a1[i,2], a1[i,3], a1[i,4], a1[i,5], a1[i,6], sep = ","), length(intersect(intersect(intersect(intersect(intersect(t1, t2), t3), t4), t5), t6))))
      }
    }
    # 創建商品組合+母QC，排除母QC為0組合
    m1 <- data.frame(mix_com = k7, p_qc = k6) 
    m1$p_qc <- str_replace_all(m1$p_qc, pattern = "[[:punct:]]", replacement = "") %>% as.numeric()
    # 母QC是否在pchome販售
    m2 <- left_join(m1 %>% mutate(p_qc = as.character(p_qc)), b4, by = c("p_qc" = "QC")) %>% group_by(mix_com) %>% summarise(pqc_buy = ifelse(sum(complete.cases(buy_count)) > 0, 1, 0))
    # 資料合併
    a1 <- cbind(a1, com_pqc_count = k5, mix_com = apply(a1, 1, function(a) str_c(a, collapse = ","))) %>% left_join(m2, by = "mix_com")
    # 篩選選需欄位(無共同母QC或無購買紀錄)
    a2 <- data.frame(lhs = k1$lhs, rhs = k1$rhs, a1[,c("com_pqc_count", "pqc_buy")], 
                     order_count = k1$count, support = k1$support, confidence = k1$confidence, lift = k1$lift) %>% 
      filter(com_pqc_count == 0) %>% 
      select(-c(pqc_buy))
    a2$parent_brand <- rep(brand, nrow(a2))
    return(a2)
  }else{
    return(a2 <- NULL)
  }
}
cat('\ncreate brand fuction: START', as.character.POSIXt(Sys.time()),'\n')
m_brand <- function(n1, n2){
  m1 <- "SELECT *
FROM(SELECT x.order_id ,
CASE WHEN x.order_id is NULL THEN NULL ELSE 'momo購物' END as display_name,
y.parent_brand,
CASE WHEN b.sub_QC is NULL THEN x.QC ELSE b.sub_QC END as QC_new,
CASE WHEN b.sub_QC is NULL THEN x.tax_price ELSE b.ec_sub_rate*x.tax_price END as new_price
FROM DataTeam.dbo.agent_sale_cost_MOMO x
LEFT JOIN DataTeam.dbo.ce_product y
ON x.QC = y.QC 
LEFT JOIN DataTeam.dbo.ce_product_tetris as b
ON  x.QC = b.QC 
WHERE y.parent_brand IN (SELECT  DISTINCT  istore_parent_brand collate chinese_taiwan_stroke_ci_as
FROM DataTeam.dbo.parent_brand_mapping
WHERE is_cooperation = 1 AND fnc = '買斷') AND x.order_day BETWEEN"
  m2 <- n1
  m3 <- "AND"
  m4 <- n2
  m5 <- "AND x.tax_price > 0 AND x.order_type = '一般訂單') as kk
WHERE kk.new_price > 0"
  uni_momo <- dbGetQuery(msconn, paste(m1,m2,m3,m4,m5))
  mo_brand <- unique(uni_momo$parent_brand)
  return(mo_brand)
}
p_brand <- function(n1, n2){
  uni_pchome <- dbGetQuery(posconn, paste0(paste0("SELECT  order_id, platform_name ,",paste0('"',"sub_QC",'"'), "AS QC_new
FROM sales.completed_orders
WHERE real_sale_price > 0 
AND platform_name = 'PChome購物'
AND order_day BETWEEN "),n1,"AND",n2)) %>% 
    left_join(product %>% mutate(QC = as.character(QC)), by = c("qc_new" = "QC")) %>% 
    rename("QC_new"="qc_new") %>% filter(parent_brand %in% brand_mapping$istore_parent_brand)
  pc_brand <- unique(uni_pchome$parent_brand)
  return(pc_brand)
}
s_brand <- function(n1, n2){
  uni_shopee <- dbGetQuery(posconn, paste0(paste0("SELECT  order_id, platform_name ,",paste0('"',"sub_QC",'"'), "AS QC_new
FROM sales.completed_orders
WHERE real_sale_price > 0 
AND platform_name = '蝦皮購物'
AND order_day BETWEEN "),n1,"AND",n2)) %>% 
    left_join(product %>% mutate(QC = as.character(QC)), by = c("qc_new" = "QC")) %>% 
    rename("QC_new"="qc_new") %>% filter(parent_brand %in% brand_mapping$istore_parent_brand)
  sp_brand <- unique(uni_shopee$parent_brand)
  return(sp_brand)
}
y_brand <- function(n1, n2){
  uni_yahoo <- dbGetQuery(posconn, paste0(paste0("SELECT  order_id, platform_name ,",paste0('"',"sub_QC",'"'), "AS QC_new
FROM sales.completed_orders
WHERE real_sale_price > 0 
AND platform_name = 'Yahoo購物'
AND order_day BETWEEN "),n1,"AND",n2)) %>% 
    left_join(product %>% mutate(QC = as.character(QC)), by = c("qc_new" = "QC")) %>% 
    rename("QC_new"="qc_new") %>% filter(parent_brand %in% brand_mapping$istore_parent_brand)
  ya_brand <- unique(uni_yahoo$parent_brand)
  return(ya_brand)
}
u_brand <- function(n1, n2){
  uni_ocb <- dbGetQuery(posconn, paste0(paste0("SELECT order_id, platform_name ,",paste0('"',"sub_QC",'"'), "AS QC_new
FROM sales.completed_orders
WHERE real_sale_price > 0 
AND platform_name NOT IN ('momo購物')
AND order_day BETWEEN "),n1,"AND",n2,paste0("UNION SELECT SUBSTRING(order_id,1,14) as order_id, platform_name ,",paste0('"',"sub_QC",'"'), "AS QC_new
FROM sales.completed_orders
WHERE real_sale_price > 0 
AND platform_name IN ('momo購物')
AND order_day BETWEEN "),n1,"AND",n2)) %>% 
    left_join(product %>% mutate(QC = as.character(QC)), by = c("qc_new" = "QC")) %>% 
    rename("QC_new"="qc_new") %>% filter(parent_brand %in% brand_mapping$istore_parent_brand)
  un_brand <- unique(uni_ocb$parent_brand)
  return(un_brand)
}
cat('\ncreate display brand: START', as.character.POSIXt(Sys.time()),'\n')
mo_brand <- m_brand(n1 = da1, n2 = da2) 
pc_brand <- p_brand(n1 = da1, n2 = da2)
sp_brand <- s_brand(n1 = da1, n2 = da2)
ya_brand <- y_brand(n1 = da1, n2 = da2)
un_brand <- u_brand(n1 = da1, n2 = da2)
cat('\ncreate momo merchandise mix: START', as.character.POSIXt(Sys.time()),'\n')
for(i in 1:length(mo_brand)){
  mo1 <- try(momo(n1 = da1, n2 = da2, brand = mo_brand[i]))
  try(
    if(class(mo1) == "try-error"){
      next
    }else if(is.null(mo1) == 1){
      next
    }else{
      mo1$QC_mix <- paste(mo1$lhs, mo1$rhs)
      mo1$display_name <- rep("momo", nrow(mo1))
      mo2 <- mo1[, c("QC_mix", "order_count", "support", "confidence", "lift", "display_name","com_pqc_count", "parent_brand")]
      merch_mix <- mo2
      merch_mix$year_month <- rep(str_sub(da1, start = 2, end = 8), nrow(merch_mix))
      merch_mo <- rbind(merch_mo, merch_mix)
    })
}
cat('\ncreate pchome merchandise mix: START', as.character.POSIXt(Sys.time()),'\n')
for(i in 1:length(pc_brand)){
  pc1 <- try(pchome(n1 = da1, n2 = da2, brand = pc_brand[i]))
  try(
    if(class(pc1) == "try-error"){
      pc2 <- next
    }else if(is.null(pc1) == 1){
      pc2 <- next
    }else{
      pc1$QC_mix <- paste(pc1$lhs, pc1$rhs)
      pc1$display_name <- rep("pchome", nrow(pc1))
      pc2 <- pc1[, c("QC_mix", "order_count", "support", "confidence", "lift", "display_name","com_pqc_count", "parent_brand")]
      merch_mix <- pc2
      merch_mix$year_month <- rep(str_sub(da1, start = 2, end = 8), nrow(merch_mix))
      merch_pc <- rbind(merch_pc, merch_mix)
    })
}
cat('\ncreate shopee merchandise mix: START', as.character.POSIXt(Sys.time()),'\n')
for(i in 1:length(sp_brand)){
  sp1 <- try(shopee(n1 = da1, n2 = da2, brand = sp_brand[i]))
  try(
    if(class(sp1) == "try-error"){
      sp2 <- next
    }else if(is.null(sp1) == 1){
      sp2 <- next
    }else{
      sp1$QC_mix <- paste(sp1$lhs, sp1$rhs)
      sp1$display_name <- rep("shopee", nrow(sp1))
      sp2 <- sp1[, c("QC_mix", "order_count", "support", "confidence", "lift", "display_name","com_pqc_count", "parent_brand")]
      merch_mix <- sp2
      merch_mix$year_month <- rep(str_sub(da1, start = 2, end = 8), nrow(merch_mix))
      merch_sp <- rbind(merch_sp, merch_mix)
    })
}
cat('\ncreate yahoo merchandise mix: START', as.character.POSIXt(Sys.time()),'\n')
for(i in 1:length(ya_brand)){
  ya1 <- try(yahoo(n1 = da1, n2 = da2, brand = ya_brand[i]))
  try(
    if(class(ya1) == "try-error"){
      ya2 <- next
    }else if(is.null(ya1) == 1){
      ya2 <- next
    }else{
      ya1$QC_mix <- paste(ya1$lhs, ya1$rhs)
      ya1$display_name <- rep("yahoo", nrow(ya1))
      ya2 <- ya1[, c("QC_mix", "order_count", "support", "confidence", "lift", "display_name","com_pqc_count", "parent_brand")]
      merch_mix <- ya2
      merch_mix$year_month <- rep(str_sub(da1, start = 2, end = 8), nrow(merch_mix))
      merch_ya <- rbind(merch_ya, merch_mix)
    })
}
cat('\ncreate undisplay merchandise mix: START', as.character.POSIXt(Sys.time()),'\n')
for(i in 1:length(un_brand)){
  un1 <- try(undisplay(n1 = da1, n2 = da2, brand = un_brand[i]))
  try(  
    if(class(un1) == "try-error"){
      un2 <- next
    }else if(is.null(un1) == 1){
      un2 <- next
    }else{
      un1$QC_mix <- paste(un1$lhs, un1$rhs)
      un1$display_name <- rep("undisplay", nrow(un1))
      un2 <- un1[, c("QC_mix", "order_count", "support", "confidence", "lift", "display_name","com_pqc_count", "parent_brand")]
      merch_mix <- un2
      merch_mix$year_month <- rep(str_sub(da1, start = 2, end = 8), nrow(merch_mix))
      merch_un <- rbind(merch_un, merch_mix)
    })
}
cat('\nmerge merchandise mix: START', as.character.POSIXt(Sys.time()),'\n')
merch <- rbind(merch_mo, merch_pc, merch_sp, merch_ya)
merch$support <- round(merch$support*100, 0)
merch$confidence <- round(merch$confidence*100, 0)
merch_un$support <- round(merch_un$support*1000, 0)
merch_un$confidence <- round(merch_un$confidence*100, 0)
merch_all <- rbind(merch_un, merch)
merch_all$lift <- round(merch_all$lift, 0)
merch_all = merch_all %>%
  mutate_at(names(which(sapply(merch_all, class) == 'character')),
            ~enc2native(.))
table_id = Id(schema = "dbo", table = "merchandise_mix")
rs1 = dbWriteTable(msconn,
                   name = table_id,
                   row.names = F,
                   header = TRUE,
                   value = merch_all,
                   append = T)

