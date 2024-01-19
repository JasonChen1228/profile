library(RSelenium)
library(tidyverse)
library(lubridate)
library(rvest)
library(magrittr)
#透過cmd驅動selenium
#輸入cd /Users/chenjiancheng/Desktop/tt/
#java -Dwebdriver.chrome.driver=chromedriver -jar selenium-server-standalone-4.0.0-alpha-2.jar
# 連接 Selenium 伺服器，選用 chrome 瀏覽器
remDr  <- remoteDriver(
  remoteServerAddr = "localhost",
  port = 4444,
  browserName = "chrome")
# 開啟瀏覽器
remDr$open()
# 瀏覽 遠傳app google評論
remDr$navigate("https://play.google.com/store/apps/details?id=com.fetself&hl=zh_TW&gl=US&showAllReviews=true")


#識別網頁正文
# 依據 CSS 選擇器取得網頁元素
webElem <- remDr$findElement("css", "body")

#向瀏覽器發送“結束”鍵以移動到正文底部
for(i in 1:20){
  message(paste("Iteration",i))
  webElem$sendKeysToElement(list(key = "end"))
  #檢查屏幕上是否存在“顯示更多”
  element<- try(unlist(remDr$findElement("class name", "RveJvd")$getElementAttribute('class')),
                silent = TRUE)
  
  #存在“顯示更多”按鈕並等待 2 秒
  Sys.sleep(2)
  if(str_detect(element, "RveJvd") == TRUE){
    buttonElem <- remDr$findElement("class name", "RveJvd")
    buttonElem$clickElement()
  }
  
  #等待 3 秒以加載新評論
  Sys.sleep(3)
}


# 重新整理
#remDr$refresh()
#找出下拉
path <- '//*[contains(concat( " ", @class, " " ), concat( " ", "DPvwYc", " " ))]'
downmenu2 <- remDr$findElement("xpath", path)
downmenu2$clickElement()

#找出最新
#//*[@id="fcxH9b"]/div[4]/c-wiz/div/div[2]/div/div/main/div/div[1]/div[2]/c-wiz/div[1]/div/div[2]/div[1]
path2 <- '//*[@id="fcxH9b"]/div[4]/c-wiz/div/div[2]/div/div/main/div/div[1]/div[2]/c-wiz/div[1]/div/div[2]/div[1]'
downmenu3 <- remDr$findElement("xpath", path2)
downmenu3$clickElement()

# 重新整理
#remDr$refresh()
#找出下拉
path <- '//*[contains(concat( " ", @class, " " ), concat( " ", "DPvwYc", " " ))]'
downmenu2 <- remDr$findElement("xpath", path)
downmenu2$clickElement()

#找出關聯性
path3 <- '//*[@id="fcxH9b"]/div[4]/c-wiz/div/div[2]/div/div/main/div/div[1]/div[2]/c-wiz/div[1]/div/div[2]/div[3]'
downmenu4 <- remDr$findElement("xpath", path3)
downmenu4$clickElement()

#找出下拉
path <- '//*[contains(concat( " ", @class, " " ), concat( " ", "DPvwYc", " " ))]'
downmenu2 <- remDr$findElement("xpath", path)
downmenu2$clickElement()

#找出最新
#//*[@id="fcxH9b"]/div[4]/c-wiz/div/div[2]/div/div/main/div/div[1]/div[2]/c-wiz/div[1]/div/div[2]/div[1]
path2 <- '//*[@id="fcxH9b"]/div[4]/c-wiz/div/div[2]/div/div/main/div/div[1]/div[2]/c-wiz/div[1]/div/div[2]/div[1]'
downmenu3 <- remDr$findElement("xpath", path2)
downmenu3$clickElement()


#加載評論
#1. 識別網頁正文
#2. 向瀏覽器發送“結束”鍵以移動到正文底部
#3. 檢查屏幕上是否存在“顯示更多”按鈕並等待 2 秒
#4. 如果該按鈕存在，則找到該元素並單擊它。
#5. 等待 3 秒以加載新評論，然後從第 2 步開始重複

#識別網頁正文
# 依據 CSS 選擇器取得網頁元素
webElem <- remDr$findElement("css", "body")

#向瀏覽器發送“結束”鍵以移動到正文底部
for(i in 1:15){
  message(paste("Iteration",i))
  webElem$sendKeysToElement(list(key = "end"))
  #檢查屏幕上是否存在“顯示更多”
  element<- try(unlist(remDr$findElement("class name", "RveJvd")$getElementAttribute('class')),
                silent = TRUE)
  
  #存在“顯示更多”按鈕並等待 2 秒
  Sys.sleep(2)
  if(str_detect(element, "RveJvd") == TRUE){
    buttonElem <- remDr$findElement("class name", "RveJvd")
    buttonElem$clickElement()
  }
  
  #等待 3 秒以加載新評論
  Sys.sleep(3)
}

#抓取頁面
html_obj <- remDr$getPageSource(header = TRUE)[[1]] %>% read_html()
#關閉伺服器
remDr$close()
system("taskkill /im java.exe /f", intern=FALSE, ignore.stdout=FALSE)

#提取評論中各個部分
#使用rvest及CSS 選擇器，以識別我想要的部分的(CSS)，並使用html_elements()、html_attr()和html_text()
# 1) 評論者姓名
names <- html_obj %>% html_elements("span.X43Kjb") %>% html_text()
#names2 <- html_obj %>% html_elements(".X43Kjb") %>% html_text()
# 2) 星星數
stars <- html_obj %>% html_elements(".kx8XBd .nt2C1d [role='img']")%>%
  html_attr("aria-label") %>%
  str_sub(start = 5, end = 5)
#3) 評論日期
dates <- html_obj %>% html_elements(".p2TkOb") %>%
  html_text() %>%
  # 轉換成日期
  ymd()
#4) 多少幫助數
clicks <- html_obj %>% html_elements('div.jUL89d.y92BAb') %>%
  html_text() %>%
  #轉換成數值
  as.integer()
# 5) 擷取評論
reviews <- html_obj %>% html_elements(".UD7Dzf") %>% html_text()
#處理"完整評論"
reviews <- if_else(
  #假如評論未顯示全部
  str_detect(reviews, '\\.\\.\\.完整評論'),
  #回填未顯示部分
  str_sub(reviews, start = str_locate(reviews, '\\.\\.\\.完整評論')[, 2]+1
  ),
  #排除空白鍵
  str_trim(reviews)
)

# 去除遠傳回覆時間及姓名
r1 <- data.frame(names = names,dates = dates)
r1 <- r1[names != "Far EasTone Telecommunications Co. Ltd",]
# 建立data.frame合併所有資料
review_data <- tibble(
  names = r1$names,
  stars = stars,
  dates = r1$dates,
  clicks = clicks,
  reviews = reviews
)


#遠傳回覆
response <- html_obj %>% html_elements(".LVQB0b") %>% html_text()
#電信名稱
response_name <- str_sub(string = response, start = 1, end = 38)
#電信回覆日期
response_date <- str_split(string = response, pattern = "Far EasTone Telecommunications Co. Ltd") %>% sapply( "[", 2)
response_date <- str_split(response_date, pattern = "日") %>% sapply( "[", 1)
response_date <- ymd(paste(response_date,rep("日",length(response_date)),sep = ""))
#電信回覆內容
response_content <- str_split(response, pattern = "日") %>% sapply( "[",2)
#合併回覆資料
response <- data.frame(name  = response_name, date = response_date,content = response_content)

t1 <- data.frame(names = names,dates = dates)
a <- data.frame()
b <- nrow(t1)-1

for(i in 1:b){
  if(t1[i+1,1] == "Far EasTone Telecommunications Co. Ltd"){
    a[i,1] <- 1
  }else{
    a[i,1] <- 0
  }
}

a[2030,1] <- 0
t2 <- cbind(t1,a)
t3 <- t2[t2$V1 ==1,]
t4 <- t2[t2$V1 ==0,]
new_response <- cbind(t3,response)
new_response[,7] <- paste(new_response$names,new_response$dates)
new_response <- new_response[,4:7]
colnames(new_response) <- c("FET_name","FET_date","FET_cont","combine")

review_data[,6] <- paste(review_data$names,review_data$dates)
colnames(review_data) <- c("rv_name","rv_stars","rv_date","rv_clicks","review","combine")
review_data1 <- merge(review_data,new_response,by = "combine",all = T)
write.csv(review_data1,"C:/Users/jasoncchen/Desktop/crawler/review_data_all.csv",row.names = F)
write.csv(new_response,"C:/Users/jasoncchen/Desktop/crawler/new_response.csv",row.names = F)
write.csv(review_data,"C:/Users/jasoncchen/Desktop/crawler/review_data.csv",row.names = F)


a1 <- read.csv("C:/Users/jasoncchen/Desktop/crawler/review_data_all.csv",header = T)


# 開啟瀏覽器
remDr$open()
# 回到上一頁
remDr$goBack()
# 前往下一頁
remDr$goForward()
# 取的目前網頁的網址
remDr$getCurrentUrl()
# 重新整理
remDr$refresh()