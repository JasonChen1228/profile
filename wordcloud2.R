#資料匯入
library(dplyr)
library(lubridate)
library(stringr)
library(jiebaR)
library(wordcloud) # 非互動式文字雲
library(wordcloud2) # 互動式文字雲
library(tm)
library(shiny)
library(shinythemes)
#匯入資料
fet <- read.csv("/Users/chenjiancheng/Downloads/評論/fet.csv",header = T)
ch <- read.csv("/Users/chenjiancheng/Downloads/評論/ch.csv",header = T)
gt <- read.csv("/Users/chenjiancheng/Downloads/評論/gt.csv",header = T)
tw <- read.csv("/Users/chenjiancheng/Downloads/評論/tw.csv",header = T)
ts <- read.csv("/Users/chenjiancheng/Downloads/評論/ts.csv",header = T)
#x1 <- str_split(fet$combine,pattern = " ")
#fet$repdate <- sapply(x1, "[", 1)
#fet <- fet[order(fet$repdate),]
#write.csv(fet,"C:/Users/jasoncchen/Desktop/crawlertest/fet.csv",row.names = F)
#選取欄位
fet1 <- fet[,-5]
ch1 <- ch[,-5]
gt1 <- gt[,-5]
tw1 <- tw[,-5]
ts1 <- ts[,-5]
#字串轉日期&新增各自電信標籤
fet1$repdate <- as.Date(as.character(fet1$repdate), format="%Y/%m/%d")
fet1$label <- rep("FET",nrow(fet1))
ch1$repdate <- as.Date(as.character(ch1$repdate), format="%Y/%m/%d")
ch1$label <- rep("CH",nrow(ch1))
gt1$repdate <- as.Date(as.character(gt1$repdate), format="%Y/%m/%d")
gt1$label <- rep("GT",nrow(gt1))
tw1$repdate <- as.Date(as.character(tw1$repdate), format="%Y/%m/%d")
tw1$label <- rep("TW",nrow(tw1))
ts1$repdate <- as.Date(as.character(ts1$repdate), format="%Y/%m/%d")
ts1$label <- rep("TS",nrow(ts1))
#合併所有評論&區分各星星數評論
total_review <- rbind(fet1,ch1,tw1,gt1,ts1)
new_words1 <- c("推薦碼","優惠碼","邀請碼","線上詢問","電信費","電信資費","電話費","電話費用",
                "永久有效","地下停車場","遠傳心生活","遠傳電信","折抵帳單","新朋友","推薦代碼",
                "優惠代碼","邀請代碼","門市人員","客服人員","遠傳幣","遠傳電信","心生活","老客戶",
                "直營店","通話費","跑馬燈","線上詢問","電信費","電信資費","電話費","電話費用",
                "中華電信","折抵帳單","新朋友","門市人員","客服人員","老客戶",
                "直營店","通話費","光世代","免出門","點進去","出來之後","點不進去",
                "好用","適當","汽車","大推","門號資訊","台灣之星","小額付款","未出帳","沒顯示",
                "為什麼","合約資訊","免手續費","資費方案","吃到飽","不穩定","小額付費","帳單明細",
                "網路用量","無法登入","連線異常","貴公司","忙線中","線上繳費","繳費明細","讀取中",
                "市話號碼","未出帳","自動繳費","指紋登入","臉部辨識","明瞭","小工具","沒反應","登入","登不進去",
                "常用設備","不方便","服務人員","自動服務","取消服務","沒進步","無法使用","申報障礙電話",
                "不便民","忘記密碼","沒辦法","不成功","實體帳單","簡訊提醒","簡訊條碼","門市人員","吃到飽",
                "顧客需求","遠傳","訊號","網路","市話","線上客服","線上報修","障礙申告","客服信箱","打不進去",
                "故障報修","上網用量","合約期限","重新登入","簡訊費","基地台","寬頻網路","連不上線","障礙維修","系統忙錄",
                "通知欄","忙線","故障申報","設備號碼","簡訊驗證","改密碼","連線逾期","線上申告","神腦","驗證信箱","操作介面",
                "亞太電信","新用戶","台灣大哥大","定期付款","台哥大","電子賬單","連線異常","簡訊帳單","停車費代繳","客服人員",
                "轉圈圈","登入","中華門號","文字客服","自動扣款","授權碼","忙碌中","忙線中","客訴","刪評論",
                "讀取中","未解決","無法打開","遠傳電信","遠傳","台星","中華","亞太",
                "代收代付","小額","加值","指紋解鎖","重開機","數據流量","數據量","刪除門號","來電答鈴","快速繳費","重新登入",
                "驗證碼","簡訊驗證碼","語音電話","小額付款","五倍券","客服中心","顯示連線逾時","連線逾時","無法操作",
                "快速繳款","收發簡訊","客戶服務信箱","客服信箱","繳費成功","繳費失敗","亞太客服","沒辦法","收訊報馬仔",
                "查詢帳單","無法連線","信號涵蓋","無法更新","驗證簡訊","認證簡訊","合約已到期","線上客服","申辦門號","收訊反應",
                "送出失敗","重新再試","強制更新","漫遊服務","下載帳單","電子發票","蝦皮購物","白屏","語音服務","工程人員","自動登出",
                "聽不清楚","不清楚","超商繳費","繳款紀錄","合約資訊","圖形登入","官網登入","登入失敗","生物辨識","提高額度","帳單地址",
                "信用卡繳款","提高額度","紙本帳單","魔術方塊","合約到期","網路傳輸量","重複繳款","線上詢問","電信費","電信資費","電話費","電話費用",
                "中華電信","折抵帳單","新朋友","門市人員","客服人員","老客戶",
                "直營店","通話費","光世代","免出門","點進去","出來之後","點不進去",
                "好用","適當","汽車","大推","門號資訊","台灣之星","小額付款","未出帳","沒顯示",
                "為什麼","合約資訊","免手續費","資費方案","吃到飽","不穩定","小額付費","帳單明細",
                "網路用量","無法登入","連線異常","貴公司","忙線中","線上繳費","繳費明細","讀取中",
                "市話號碼","未出帳","自動繳費","指紋登入","臉部辨識","明瞭","小工具","沒反應","登入","登不進去",
                "常用設備","不方便","服務人員","自動服務","取消服務","沒進步","無法使用","申報障礙電話",
                "不便民","忘記密碼","沒辦法","不成功","實體帳單","簡訊提醒","簡訊條碼","門市人員","吃到飽",
                "顧客需求","遠傳","訊號","網路","市話","線上客服","線上報修","障礙申告","客服信箱","打不進去",
                "故障報修","上網用量","合約期限","重新登入","簡訊費","基地台","寬頻網路","連不上線","障礙維修","系統忙錄",
                "通知欄","忙線","故障申報","設備號碼","簡訊驗證","改密碼","連線逾期","線上申告","神腦","驗證信箱","操作介面",
                "亞太電信","新用戶","台灣大哥大","定期付款","台哥大","電子賬單","連線異常","簡訊帳單","停車費代繳","客服人員",
                "轉圈圈","登入","中華門號","文字客服","自動扣款","授權碼","忙碌中","忙線中","客訴","刪評論",
                "讀取中","未解決","無法打開","遠傳電信","遠傳","台星","中華","亞太",
                "代收代付","小額","加值","指紋解鎖","重開機","數據流量","數據量","刪除門號","來電答鈴","快速繳費","重新登入",
                "驗證碼","簡訊驗證碼","語音電話","小額付款","五倍券","客服中心","顯示連線逾時","連線逾時","無法操作",
                "快速繳款","收發簡訊","客戶服務信箱","客服信箱","繳費成功","繳費失敗","亞太客服","沒辦法","收訊報馬仔",
                "查詢帳單","無法連線","信號涵蓋","無法更新","驗證簡訊","認證簡訊","合約已到期","線上客服","申辦門號","收訊反應",
                "送出失敗","重新再試","強制更新","漫遊服務","下載帳單","電子發票","蝦皮購物","白屏","語音服務","工程人員","自動登出",
                "聽不清楚","不清楚","超商繳費","繳款紀錄","合約資訊","圖形登入","官網登入","登入失敗","生物辨識","提高額度","帳單地址",
                "信用卡繳款","提高額度","紙本帳單","魔術方塊","合約到期","網路傳輸量","重複繳款",
                "優惠方案","操作簡單","使用流量","本期通話時間","剩餘用量","介面","轉帳繳費","轉帳","好評","手機鈴聲","網路用量","不限速",
                "新版本","信用卡繳費","教學軟體","本機門號","自動登入","智能續約優惠","無法打開","智能客服","智障客服","優惠序號","通話金",
                "對話視窗","多門號","繳款紀錄","客服專線","優惠專案","再次登入","金融卡扣款","金融卡","網內免費","超商補單繳費","補單繳費")
new_words1 <- unique(new_words1)
#停止詞
stop_words <- c("在","的","個","了","又","再","呢","是","喔","喲","唷","啊","啦","嗯","唉","很","都")
#設定選擇電信及好/負評
telecom <- c("FET","CH","TW","GT","TS" )
cont <- c("good","bad")
dat1 <- min(total_review$repdate)
dat2 <- Sys.time()
haha <- function(star,label,dat1,dat2){
  if(star == "good"){
    a1 <- total_review[total_review$star>=3 & total_review$repdate >= dat1 & total_review$repdate <= dat2,]
    if(label == "FET"){
      a3 <- a1[a1$label == "FET",]
      a4 <- a3$review
      cutter <- worker(user = "new_words1.txt", stop_word = "stop_words.txt", bylines = FALSE)
      for (i in 1:length(new_words1)) {new_user_word(cutter, new_words1[i])}
      a4 <- str_remove_all(a4, "[0-9a-zA-Z]+?")
      seg_words <- cutter[a4]
      for(i in 1:length(stop_words)){
        seg_words <- str_replace_all(seg_words,stop_words[i],"")
      }
      seg_words <- seg_words[which(nchar(seg_words)>0)]
      txt_freq <- freq(seg_words)
      x1 <- txt_freq[nchar(txt_freq$char)>1 & txt_freq$freq>2,]
      return(x1)
    }else if(label == "CH" ){
      a3 <- a1[a1$label == "CH",]
      a4 <- a3$review
      cutter <- worker(user = "new_words1.txt", stop_word = "stop_words.txt", bylines = FALSE)
      for (i in 1:length(new_words1)) {new_user_word(cutter, new_words1[i])}
      a4 <- str_remove_all(a4, "[0-9a-zA-Z]+?")
      seg_words <- cutter[a4]
      for(i in 1:length(stop_words)){
        seg_words <- str_replace_all(seg_words,stop_words[i],"")
      }
      seg_words <- seg_words[which(nchar(seg_words)>0)]
      txt_freq <- freq(seg_words)
      x1 <- txt_freq[nchar(txt_freq$char)>1 & txt_freq$freq>2,]
      return(x1)
    }else if(label == "GT" ){
      a3 <- a1[a1$label == "GT",]
      a4 <- a3$review
      cutter <- worker(user = "new_words1.txt", stop_word = "stop_words.txt", bylines = FALSE)
      for (i in 1:length(new_words1)) {new_user_word(cutter, new_words1[i])}
      a4 <- str_remove_all(a4, "[0-9a-zA-Z]+?")
      seg_words <- cutter[a4]
      for(i in 1:length(stop_words)){
        seg_words <- str_replace_all(seg_words,stop_words[i],"")
      }
      seg_words <- seg_words[which(nchar(seg_words)>0)]
      txt_freq <- freq(seg_words)
      x1 <- txt_freq[nchar(txt_freq$char)>1 & txt_freq$freq>2,]
      return(x1)
    }else if(label == "TW" ){
      a3 <- a1[a1$label == "TW",]
      a4 <- a3$review
      cutter <- worker(user = "new_words1.txt", stop_word = "stop_words.txt", bylines = FALSE)
      for (i in 1:length(new_words1)) {new_user_word(cutter, new_words1[i])}
      a4 <- str_remove_all(a4, "[0-9a-zA-Z]+?")
      seg_words <- cutter[a4]
      for(i in 1:length(stop_words)){
        seg_words <- str_replace_all(seg_words,stop_words[i],"")
      }
      seg_words <- seg_words[which(nchar(seg_words)>0)]
      txt_freq <- freq(seg_words)
      x1 <- txt_freq[nchar(txt_freq$char)>1 & txt_freq$freq>2,]
      return(x1)
    }else{
      a3 <- a1[a1$label == "TS",]
      a4 <- a3$review
      cutter <- worker(user = "new_words1.txt", stop_word = "stop_words.txt", bylines = FALSE)
      for (i in 1:length(new_words1)) {new_user_word(cutter, new_words1[i])}
      a4 <- str_remove_all(a4, "[0-9a-zA-Z]+?")
      seg_words <- cutter[a4]
      for(i in 1:length(stop_words)){
        seg_words <- str_replace_all(seg_words,stop_words[i],"")
      }
      seg_words <- seg_words[which(nchar(seg_words)>0)]
      txt_freq <- freq(seg_words)
      x1 <- txt_freq[nchar(txt_freq$char)>1 & txt_freq$freq>2,]
      return(x1)
    }
  }else{
    a2 <- total_review[total_review$star<3 & total_review$repdate >= dat1 & total_review$repdate <= dat2,]
    if(label == "FET"){
      a3 <- a2[a2$label == "FET",]
      a4 <- a3$review
      cutter <- worker(user = "new_words1.txt", stop_word = "stop_words.txt", bylines = FALSE)
      for (i in 1:length(new_words1)) {new_user_word(cutter, new_words1[i])}
      a4 <- str_remove_all(a4, "[0-9a-zA-Z]+?")
      seg_words <- cutter[a4]
      for(i in 1:length(stop_words)){
        seg_words <- str_replace_all(seg_words,stop_words[i],"")
      }
      seg_words <- seg_words[which(nchar(seg_words)>0)]
      txt_freq <- freq(seg_words)
      x1 <- txt_freq[nchar(txt_freq$char)>1 & txt_freq$freq>2,]
      return(x1)
    }else if(label == "CH" ){
      a3 <- a2[a2$label == "CH",]
      a4 <- a3$review
      cutter <- worker(user = "new_words1.txt", stop_word = "stop_words.txt", bylines = FALSE)
      for (i in 1:length(new_words1)) {new_user_word(cutter, new_words1[i])}
      a4 <- str_remove_all(a4, "[0-9a-zA-Z]+?")
      seg_words <- cutter[a4]
      for(i in 1:length(stop_words)){
        seg_words <- str_replace_all(seg_words,stop_words[i],"")
      }
      seg_words <- seg_words[which(nchar(seg_words)>0)]
      txt_freq <- freq(seg_words)
      x1 <- txt_freq[nchar(txt_freq$char)>1 & txt_freq$freq>2,]
      return(x1)
    }else if(label == "GT" ){
      a3 <- a2[a2$label == "GT",]
      a4 <- a3$review
      cutter <- worker(user = "new_words1.txt", stop_word = "stop_words.txt", bylines = FALSE)
      for (i in 1:length(new_words1)) {new_user_word(cutter, new_words1[i])}
      a4 <- str_remove_all(a4, "[0-9a-zA-Z]+?")
      seg_words <- cutter[a4]
      for(i in 1:length(stop_words)){
        seg_words <- str_replace_all(seg_words,stop_words[i],"")
      }
      seg_words <- seg_words[which(nchar(seg_words)>0)]
      txt_freq <- freq(seg_words)
      x1 <- txt_freq[nchar(txt_freq$char)>1 & txt_freq$freq>2,]
      return(x1)
    }else if(label == "TW" ){
      a3 <- a2[a2$label == "TW",]
      a4 <- a3$review
      cutter <- worker(user = "new_words1.txt", stop_word = "stop_words.txt", bylines = FALSE)
      for (i in 1:length(new_words1)) {new_user_word(cutter, new_words1[i])}
      a4 <- str_remove_all(a4, "[0-9a-zA-Z]+?")
      seg_words <- cutter[a4]
      for(i in 1:length(stop_words)){
        seg_words <- str_replace_all(seg_words,stop_words[i],"")
      }
      seg_words <- seg_words[which(nchar(seg_words)>0)]
      txt_freq <- freq(seg_words)
      x1 <- txt_freq[nchar(txt_freq$char)>1 & txt_freq$freq>2,]
      return(x1)
    }else{
      a3 <- a2[a2$label == "TS",]
      a4 <- a3$review
      cutter <- worker(user = "new_words1.txt", stop_word = "stop_words.txt", bylines = FALSE)
      for (i in 1:length(new_words1)) {new_user_word(cutter, new_words1[i])}
      a4 <- str_remove_all(a4, "[0-9a-zA-Z]+?")
      seg_words <- cutter[a4]
      for(i in 1:length(stop_words)){
        seg_words <- str_replace_all(seg_words,stop_words[i],"")
      }
      seg_words <- seg_words[which(nchar(seg_words)>0)]
      txt_freq <- freq(seg_words)
      x1 <- txt_freq[nchar(txt_freq$char)>1 & txt_freq$freq>2,]
      return(x1)
    }
  }
}
#顯示好/負評最常出現文字前10名
dat3 <- min(total_review$repdate)
dat4 <- Sys.time()
ha <- function(star,label,dat3,dat4){
  if(star == "good"){
    if(label == "FET"){
      t1 <- data.frame(haha("good","FET",dat3,dat4))
      t1 <- t1[order(t1$freq,decreasing = T),]
      t2 <- total_review[total_review$label =="FET" & total_review$star>=3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t3 <- total_review[total_review$label =="FET" & total_review$star<3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t4 <- total_review[total_review$label =="FET" & 
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      cat(cat("遠傳總筆數:",nrow(t4),"筆","\n",
              "抓取區間:",year(dat3),"/",month(dat3),"/",day(dat3),"~",year(dat4),"/",month(dat4),"/",day(dat4),"\n",
              "遠傳好評數(3星以上):",nrow(t2),"筆"," ","佔比:",paste(round(nrow(t2)/nrow(t4),4)*100,"%",sep=""),"\n",
              "遠傳負評數(3星以下):",nrow(t3),"筆"," ","佔比:",paste(round(nrow(t3)/nrow(t4),4)*100,"%",sep=""),"\n",
              "好評關鍵字(筆數及佔比):","\n",
              sep=""),
          for(i in 1:10){
            cat(t1[i,1],"筆數:",t1[i,2]," ","佔比:",paste(round(t1[i,2]/nrow(t2),4)*100,"%",sep = ""),"\n",sep = "")  
          }
      )
    }else if(label == "CH"){
      t1 <- data.frame(haha("good","CH",dat3,dat4))
      t1 <- t1[order(t1$freq,decreasing = T),]
      t2 <- total_review[total_review$label =="CH" & total_review$star>=3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t3 <- total_review[total_review$label =="CH" & total_review$star<3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t4 <- total_review[total_review$label =="CH" & 
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      cat(cat("中華總筆數:",nrow(t4),"筆","\n",
              "抓取區間:",year(dat3),"/",month(dat3),"/",day(dat3),"~",year(dat4),"/",month(dat4),"/",day(dat4),"\n",
              "中華好評數(3星以上):",nrow(t2),"筆"," ","佔比:",paste(round(nrow(t2)/nrow(t4),4)*100,"%",sep=""),"\n",
              "中華負評數(3星以下):",nrow(t3),"筆"," ","佔比:",paste(round(nrow(t3)/nrow(t4),4)*100,"%",sep=""),"\n",
              "好評關鍵字(筆數及佔比):","\n",sep=""),
          for(i in 1:10){
            cat(t1[i,1],"筆數:",t1[i,2]," ","佔比:",paste(round(t1[i,2]/nrow(t2),4)*100,"%",sep = ""),"\n",sep = "")  
          }
      )
    }else if(label == "GT"){
      t1 <- data.frame(haha("good","GT",dat3,dat4))
      t1 <- t1[order(t1$freq,decreasing = T),]
      t2 <- total_review[total_review$label =="GT" & total_review$star>=3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t3 <- total_review[total_review$label =="GT" & total_review$star<3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t4 <- total_review[total_review$label =="GT" & 
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      cat(cat("亞太總筆數:",nrow(t4),"筆","\n",
              "抓取區間:",year(dat3),"/",month(dat3),"/",day(dat3),"~",year(dat4),"/",month(dat4),"/",day(dat4),"\n",
              "亞太好評數(3星以上):",nrow(t2),"筆"," ","佔比:",paste(round(nrow(t2)/nrow(t4),4)*100,"%",sep=""),"\n",
              "亞太負評數(3星以下):",nrow(t3),"筆"," ","佔比:",paste(round(nrow(t3)/nrow(t4),4)*100,"%",sep=""),"\n",
              "好評關鍵字(筆數及佔比):","\n",sep=""),
          for(i in 1:10){
            cat(t1[i,1],"筆數:",t1[i,2]," ","佔比:",paste(round(t1[i,2]/nrow(t2),4)*100,"%",sep = ""),"\n",sep = "")  
          }
      )
    }else if(label == "TW"){
      t1 <- data.frame(haha("good","TW",dat3,dat4))
      t1 <- t1[order(t1$freq,decreasing = T),]
      t2 <- total_review[total_review$label =="TW" & total_review$star>=3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t3 <- total_review[total_review$label =="TW" & total_review$star<3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t4 <- total_review[total_review$label =="TW" & 
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      cat(cat("台哥大總筆數:",nrow(t4),"筆","\n",
              "抓取區間:",year(dat3),"/",month(dat3),"/",day(dat3),"~",year(dat4),"/",month(dat4),"/",day(dat4),"\n",
              "台哥大好評數(3星以上):",nrow(t2),"筆"," ","佔比:",paste(round(nrow(t2)/nrow(t4),4)*100,"%",sep=""),"\n",
              "台哥大負評數(3星以下):",nrow(t3),"筆"," ","佔比:",paste(round(nrow(t3)/nrow(t4),4)*100,"%",sep=""),"\n",
              "好評關鍵字(筆數及佔比):","\n",sep=""),
          for(i in 1:10){
            cat(t1[i,1],"筆數:",t1[i,2]," ","佔比:",paste(round(t1[i,2]/nrow(t2),4)*100,"%",sep = ""),"\n",sep = "")  
          }
      )
    }else{
      t1 <- data.frame(haha("good","TS",dat3,dat4))
      t1 <- t1[order(t1$freq,decreasing = T),]
      t2 <- total_review[total_review$label =="TS" & total_review$star>=3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t3 <- total_review[total_review$label =="TS" & total_review$star<3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t4 <- total_review[total_review$label =="TS" & 
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      cat(cat("台星總筆數:",nrow(t4),"筆","\n",
              "抓取區間:",year(dat3),"/",month(dat3),"/",day(dat3),"~",year(dat4),"/",month(dat4),"/",day(dat4),"\n",
              "台星好評數(3星以上):",nrow(t2),"筆"," ","佔比:",paste(round(nrow(t2)/nrow(t4),4)*100,"%",sep=""),"\n",
              "台星負評數(3星以下):",nrow(t3),"筆"," ","佔比:",paste(round(nrow(t3)/nrow(t4),4)*100,"%",sep=""),"\n",
              "好評關鍵字(筆數及佔比):","\n",sep=""),
          for(i in 1:10){
            cat(t1[i,1],"筆數:",t1[i,2]," ","佔比:",paste(round(t1[i,2]/nrow(t2),4)*100,"%",sep = ""),"\n",sep = "")  
          }
      )
    }
  }else{
    if(label == "FET"){
      t1 <- data.frame(haha("bad","FET",dat3,dat4))
      t1 <- t1[order(t1$freq,decreasing = T),]
      t2 <- total_review[total_review$label =="FET" & total_review$star>=3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t3 <- total_review[total_review$label =="FET" & total_review$star<3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t4 <- total_review[total_review$label =="FET" & 
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      cat(cat("遠傳總筆數:",nrow(t4),"筆","\n",
              "抓取區間:",year(dat3),"/",month(dat3),"/",day(dat3),"~",year(dat4),"/",month(dat4),"/",day(dat4),"\n",
              "遠傳好評數(3星以上):",nrow(t2),"筆"," ","佔比:",paste(round(nrow(t2)/nrow(t4),4)*100,"%",sep=""),"\n",
              "遠傳負評數(3星以下):",nrow(t3),"筆"," ","佔比:",paste(round(nrow(t3)/nrow(t4),4)*100,"%",sep=""),"\n",
              "負評關鍵字(筆數及佔比):","\n",sep=""),
          for(i in 1:10){
            cat(t1[i,1],"筆數:",t1[i,2]," ","佔比:",paste(round(t1[i,2]/nrow(t3),4)*100,"%",sep = ""),"\n",sep = "")  
          }
      )
    }else if(label == "CH"){
      t1 <- data.frame(haha("bad","CH",dat3,dat4))
      t1 <- t1[order(t1$freq,decreasing = T),]
      t2 <- total_review[total_review$label =="CH" & total_review$star>=3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t3 <- total_review[total_review$label =="CH" & total_review$star<3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t4 <- total_review[total_review$label =="CH" & 
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      cat(cat("中華總筆數:",nrow(t4),"筆","\n",
              "抓取區間:",year(dat3),"/",month(dat3),"/",day(dat3),"~",year(dat4),"/",month(dat4),"/",day(dat4),"\n",
              "中華好評數(3星以上):",nrow(t2),"筆"," ","佔比:",paste(round(nrow(t2)/nrow(t4),4)*100,"%",sep=""),"\n",
              "中華負評數(3星以下):",nrow(t3),"筆"," ","佔比:",paste(round(nrow(t3)/nrow(t4),4)*100,"%",sep=""),"\n",
              "負評關鍵字(筆數及佔比):","\n",sep=""),
          for(i in 1:10){
            cat(t1[i,1],"筆數:",t1[i,2]," ","佔比:",paste(round(t1[i,2]/nrow(t3),4)*100,"%",sep = ""),"\n",sep = "")  
          }
      )
    }else if(label == "GT"){
      t1 <- data.frame(haha("bad","GT",dat3,dat4))
      t1 <- t1[order(t1$freq,decreasing = T),]
      t2 <- total_review[total_review$label =="GT" & total_review$star>=3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t3 <- total_review[total_review$label =="GT" & total_review$star<3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t4 <- total_review[total_review$label =="GT" & 
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      cat(cat("亞太總筆數:",nrow(t4),"筆","\n",
              "抓取區間:",year(dat3),"/",month(dat3),"/",day(dat3),"~",year(dat4),"/",month(dat4),"/",day(dat4),"\n",
              "亞太好評數(3星以上):",nrow(t2),"筆"," ","佔比:",paste(round(nrow(t2)/nrow(t4),4)*100,"%",sep=""),"\n",
              "亞太負評數(3星以下):",nrow(t3),"筆"," ","佔比:",paste(round(nrow(t3)/nrow(t4),4)*100,"%",sep=""),"\n",
              "負評關鍵字(筆數及佔比):","\n",sep=""),
          for(i in 1:10){
            cat(t1[i,1],"筆數:",t1[i,2]," ","佔比:",paste(round(t1[i,2]/nrow(t3),4)*100,"%",sep = ""),"\n",sep = "")  
          }
      )
    }else if(label == "TW"){
      t1 <- data.frame(haha("bad","TW",dat3,dat4))
      t1 <- t1[order(t1$freq,decreasing = T),]
      t2 <- total_review[total_review$label =="TW" & total_review$star>=3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t3 <- total_review[total_review$label =="TW" & total_review$star<3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t4 <- total_review[total_review$label =="TW" & 
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      cat(cat("台哥大總筆數:",nrow(t4),"筆","\n",
              "抓取區間:",year(dat3),"/",month(dat3),"/",day(dat3),"~",year(dat4),"/",month(dat4),"/",day(dat4),"\n",
              "台哥大好評數(3星以上):",nrow(t2),"筆"," ","佔比:",paste(round(nrow(t2)/nrow(t4),4)*100,"%",sep=""),"\n",
              "台哥大負評數(3星以下):",nrow(t3),"筆"," ","佔比:",paste(round(nrow(t3)/nrow(t4),4)*100,"%",sep=""),"\n",
              "負評關鍵字(筆數及佔比):","\n",sep=""),
          for(i in 1:10){
            cat(t1[i,1],"筆數:",t1[i,2]," ","佔比:",paste(round(t1[i,2]/nrow(t2),4)*100,"%",sep = ""),"\n",sep = "")  
          }
      )
    }else{
      t1 <- data.frame(haha("bad","TS",dat3,dat4))
      t1 <- t1[order(t1$freq,decreasing = T),]
      t2 <- total_review[total_review$label =="TS" & total_review$star>=3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t3 <- total_review[total_review$label =="TS" & total_review$star<3 &
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      t4 <- total_review[total_review$label =="TS" & 
                           total_review$repdate >= dat3 & total_review$repdate <= dat4,]
      cat(cat("台星總筆數:",nrow(t4),"筆","\n",
              "抓取區間:",year(dat3),"/",month(dat3),"/",day(dat3),"~",year(dat4),"/",month(dat4),"/",day(dat4),"\n",
              "台星好評數(3星以上):",nrow(t2),"筆"," ","佔比:",paste(round(nrow(t2)/nrow(t4),4)*100,"%",sep=""),"\n",
              "台星負評數(3星以下):",nrow(t3),"筆"," ","佔比:",paste(round(nrow(t3)/nrow(t4),4)*100,"%",sep=""),"\n",
              "負評關鍵字(筆數及佔比):","\n",sep=""),
          for(i in 1:10){
            cat(t1[i,1],"筆數:",t1[i,2]," ","佔比:",paste(round(t1[i,2]/nrow(t3),4)*100,"%",sep = ""),"\n",sep = "")  
          }
      )
    }
  }
}
ui <- fluidPage(
  themeSelector(),
  title = "Telecom APP on Google Play Store Rivew",
  sidebarPanel(h4("Telecom APP on Google Play Store Rivew"),
               selectInput("telecom1",label = "choose a telecom_top",choices = telecom),
               selectInput('content1',label = "choose a comment_top",choices = cont),
               selectInput("telecom2",label = "choose a telecom_down",choices = telecom),
               selectInput('content2',label = "choose a comment_down",choices = cont),
               dateRangeInput("dates", label = h5("Date range"),start = min(total_review$repdate),end = Sys.time())),
  mainPanel(tabsetPanel(type = "tabs",
                        tabPanel("Plot", 
                                 wordcloud2Output('plot1',height = 400),
                                 hr(),
                                 wordcloud2Output('plot2',height = 400)),
                        tabPanel("Stats",fluidRow(column(6,verbatimTextOutput("stats1")),
                                                  column(6,verbatimTextOutput("stats2"))) 
                        ))))
server <- function(input, output) {
  output$stats1 <- renderPrint(ha(input$content1,input$telecom1,input$dates[1],input$dates[2]))
  output$stats2 <- renderPrint(ha(input$content2,input$telecom2,input$dates[1],input$dates[2]))
  output$plot1 <- renderWordcloud2(
    wordcloud2(haha(input$content1,input$telecom1,input$dates[1],input$dates[2]),
               shape = "circle",fontFamily = "Microsoft YaHei", size = 1))
  output$plot2 <- renderWordcloud2(
    wordcloud2(haha(input$content2,input$telecom2,input$dates[1],input$dates[2]),
               shape = "circle",fontFamily = "Microsoft YaHei", size = 1))
}
shinyApp(ui = ui, server = server)
getwd()
