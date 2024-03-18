from sql_analysis import *
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd


def scrape_analysis(url, domain, web_headers):
    # 設置 Chrome 瀏覽器選項
    chrome_options = Options()

    # 初始化 Chrome 瀏覽器（chromedriver 為放置路徑，以下路徑為放在程式同一個資料夾，請根據您的狀況來設定）
    driver = webdriver.Chrome('./chromedriver.exe', options=chrome_options)

    try:
        driver.get(url)

        # 等待網頁加載（如果有需要的話）
        # 可以使用以下方式，但需要先 import time
        # time.sleep(5)

        # 等待網頁加載（使用 Selenium 內置的等待）
        # By 是 selenium 內建的一個 class，這個 class 內建很多 method 來定位元素
        # WebDriverWait 和 excepted_conditions 也是類別，裡面有 method 可調用
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        wait = WebDriverWait(driver, 10)
        # 確認等待網頁內容出現
        # 直到 css selector 的內容出現 class='highcharts-label highcharts-data-label highcharts-data-label-color-0'
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, '.highcharts-label.highcharts-data-label.highcharts-data-label-color-0')))

        # 獲取網頁內容
        page_source = driver.page_source
        # with open('./data.txt', 'w') as file:
        #    file.write(page_source)

        # 使用 BeautifulSoup 解析網頁內容
        soup = BeautifulSoup(page_source, 'html.parser')
        # 發現 wa-traffic-sources__channels-data-label 是屬於 Marketing Channels 專屬的，
        # 但 .highcharts-label .highcharts-data-label .highcharts-data-label-color-0 則是所有數據元素都有（不僅是 Marketing Channels）
        # 但其他數據資料沒有 wa-traffic-sources__channels-data-label，所以直接 select，for 迴圈內就不需要再另外使用 css 選擇器了
        marketing = soup.select(
            '.wa-traffic-sources__channels-data-label')
        data = {}
        data['網站名稱'] = domain
        # 刪除 web_headers 中的'網站名稱'
        df = web_headers[1:]

        # 只會有一個 url 的七種 channel 的 percent 分配
        # 所以這個 for 迴圈，會把七種的 percent 依序爬出來
        # 同時將兩個可迭代物件配對，就可以同時迭代 marketing 和 web_headers
        print(zip(marketing, df))
        for method, index in zip(marketing, df):
            # class 名稱前面要加上 .classname 才是正確選擇器
            # strip()：去除頭尾空格
            percent = method.text.strip()
            if percent == '<0.01%':
                percent = '0.00%'
            # 整理成 data
            data[index] = percent

        print(data)
        return data

    finally:
        # 關閉瀏覽器
        driver.quit()
