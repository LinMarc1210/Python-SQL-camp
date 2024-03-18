from web_analysis import *
from sql_analysis import *
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
from pandasql import sqldf
import matplotlib.pyplot as plt

# 讓中文資料也能依照對齊的格式輸出
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
# 指定中文字體的設定
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False


# 定義抓取程式（抓排行榜）
def scrape_similarweb(url):

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
        # 直到 css selector 的內容出現 class='.tw-table__row'
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, '.tw-table__row')))

        # 獲取網頁內容
        page_source = driver.page_source

        # 使用 BeautifulSoup 解析網頁內容
        soup = BeautifulSoup(page_source, 'html.parser')
        sites = soup.select('.tw-table__row')
        row_list = []
        for site in sites:
            # class 名稱前面要加上 .class name 才是正確選擇器
            # strip()：去除頭尾空格
            rank = site.select_one('.tw-table__row-rank').text.strip()
            analysis_link = site.select_one(
                '.tw-table__row-compare').get_attribute_list('href')        # get_attribute_list 是回傳一個 list 喔！
            domain = site.select_one('.tw-table__row-domain').text.strip()
            category = site.select_one('.tw-table__row-category').text.strip()
            rank_change = site.select_one(
                '.tw-table__row-rank-change').text.strip()
            visit_duration = site.select_one(
                '.tw-table__row-avg-visit-duration').text.strip()
            visit_pages = site.select_one(
                '.tw-table__row-pages-per-visit').text.strip()
            bounce_rate = site.select_one(
                '.tw-table__row-bounce-rate').text.strip()

            # 檢查是否有爬到：
            """
            print(f"Rank: {rank}")
            print(f"Analysis Link: {analysis_link}")
            print(f"Domain: {domain}")
            print(f"Category: {category}")
            print(f"Rank Change: {rank_change}")
            print(f"Avg. Visit Duration: {visit_duration}")
            print(f"Pages Per Visit: {visit_pages}")
            print(f"Bounce Rate: {bounce_rate}")
            print("-----------------------------")
            """

            # 整理成 data
            # https://www.similarweb.com/zh-tw/website/google.com/#overview  是 analysis_link 的完整格式
            data = {}
            data['rank'] = rank
            data['analysis_link'] = f'https://www.similarweb.com{analysis_link[0]}#overview'
            data['domain'] = domain
            data['category'] = category
            data['rank_change'] = rank_change
            data['visit_duration'] = visit_duration
            data['visit_pages'] = visit_pages
            data['bounce_rate'] = bounce_rate
            row_list.append(data)

        return row_list

    finally:
        # 關閉瀏覽器
        driver.quit()


# 整理成 csv 檔案，且回傳其 df
def write_to_csv(row_list, headers, filename, index):
    df = pd.DataFrame(row_list, columns=headers)
    df = df.set_index(index)
    df.to_csv(filename)
    return df

# 繪成圖表


def drawing_chart(df, dict):
    df.plot(kind='bar')
    plt.title(dict['title'])
    plt.xlabel(dict['xlabel'])
    plt.ylabel(dict['ylabel'])
    plt.savefig(dict['filename'])


# 呼叫函式
url = 'https://www.similarweb.com/zh-tw/top-websites/'
row_list = scrape_similarweb(url)
headers = ['rank', 'analysis_link', 'domain', 'category', 'rank_change',
           'visit_duration', 'visit_pages', 'bounce_rate']
filename = 'sites.csv'
df = write_to_csv(row_list, headers, filename, 'rank')
# print(df)

# 使用自創模組 sql_analysis 進行資料庫分析
print('\n==== count of category ====')
print(category_count(df))
print('\n==== visit info of each category ====')
print(visit_info(df))
print('\n==== total rank change of each category ====')
print(avg_bounce_rate(df))
print('\n==== sum of rank in each category ====')
print(sum_of_rank(df))
print('\n==== Quantitative Analysis：量化分析熱門分類 ====')
df_quantify = quantify(df)
print(df_quantify)
df_quantify = df_quantify.loc[:, ['TOTAL_POINT']]
print(df_quantify)
# print(df.loc["1", "analysis_link"])
quantify_dict = {'title': '量化分析（依分類）',
                 'xlabel': '項目',
                 'ylabel': '分數',
                 'filename': 'category_score.png'
                 }
drawing_chart(df_quantify, quantify_dict)


# 使用自創模組 web_analysis 進行其中一個類別的個別網站資訊爬蟲
# 選擇一個分類的網頁比較
df_category = select_category(df, "電腦電子與科技 > 社群媒體網路")
analysis_list = []
web_headers = ['網站名稱', '直接', '引薦', '自然搜尋', '付費搜尋', '社群', '郵件', '多媒體廣告']
filename_category = 'analysis.csv'
df_category = df_category.loc[:, [
    "analysis_link", "domain"]].set_index('analysis_link')
print(df_category)

for index, row in df_category.iterrows():
    url = index
    domain = row['domain']
    # 與 scrape_similarweb 不同的是，scrape_analysis 是回傳一個 dict，相當於前面提及的 row_list 的其中一行
    data = scrape_analysis(url, domain, web_headers)
    analysis_list.append(data)

df_analysis = write_to_csv(analysis_list, web_headers,
                           filename_category, '網站名稱')

chart_dict = {'title': '《電腦電子與科技 > 社群媒體網路》的網站比較',
              'xlabel': '行銷管道分布',
              'ylabel': '百分比',
              'filename': 'Comparison.png'
              }

for i in ["直接", "引薦", "自然搜尋", "付費搜尋", "社群", "郵件", "多媒體廣告"]:
    df_analysis[i] = pd.to_numeric(df_analysis[i].str.replace('%', ''))

drawing_chart(df_analysis, chart_dict)
