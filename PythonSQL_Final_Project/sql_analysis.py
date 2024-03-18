# 這個 module 是用來在 dataframe 進行 sql 分析
from pandasql import sqldf
import pandas as pd
# pysqldf 接受一個 sql 查詢作為輸入，然後回傳一個 dataframe，就不用每次都寫成 sqldf( sql, locals() )


# 選出其中一個類別的所有進前 50 的網站
def select_category(src, cat):
    sql = """
    SELECT
        domain,
        analysis_link
    FROM src
    WHERE category = '{cat}'
    """
    sql = sql.format(cat=cat)
    result = sqldf(sql)
    return result


# 計算每個 category 有幾個網站進前 50
def category_count(src):
    sql = """
    SELECT 
        category,
        COUNT(category) as category_count
    FROM src
    GROUP BY category
    """
    result = sqldf(sql, locals()).set_index(
        'category').sort_values('category_count', ascending=False)
    return result


# 計算每個 category 的平均瀏覽時間和平均瀏覽頁數
def visit_info(src):
    # 將時間轉成 timedelta 型別以便計算
    src['visit_duration'] = pd.to_timedelta(src['visit_duration'])
    # 將 visit_duration 轉換為秒數，另創一個欄位：visit_duration_seconds
    src['visit_duration_seconds'] = src['visit_duration'].dt.total_seconds()
    sql = """
    SELECT
        category,
        AVG(visit_duration_seconds) as avg_category_time,
        AVG(visit_pages) as avg_category_pages
    FROM src
    GROUP BY category
    """
    result = sqldf(sql, locals()).set_index('category')

    # 轉換總秒數為 timedelta 型別
    result['avg_category_time'] = pd.to_timedelta(
        result['avg_category_time'], unit='s')

    # 刪除中間產生的 visit_duration_seconds 欄位
    src.drop('visit_duration_seconds', axis=1, inplace=True)

    return result


# 計算每個 category 的平均彈出率（離開網站前僅瀏覽一個頁面的訪客百分比）
def avg_bounce_rate(src):
    sql = """
    SELECT
        category,
        AVG(bounce_rate) as avg_bounce_rate_in_percent
    FROM src 
    GROUP BY category
    """
    result = sqldf(sql, locals()).set_index('category').sort_values(
        'avg_bounce_rate_in_percent', ascending=True)
    return result


# 計算每個 category 的總排行（加總），以分數小為最好
def sum_of_rank(src):
    sql = """
    SELECT
        category,
        SUM(rank) as sum_of_rank_by_category
    FROM src
    GROUP BY category
    """
    result = sqldf(sql, locals()).set_index('category').sort_values(
        'sum_of_rank_by_category', ascending=True)
    return result


# 量化分析
def quantify(src):
    table_count = category_count(src)
    table_visit = visit_info(src)
    table_bounce = avg_bounce_rate(src)
    table_rank = sum_of_rank(src)
    # 使用 INNER JOIN ... USING(同樣的欄位名)，可以避免 join 之後出現同樣的欄位
    sql = """
    SELECT *
    FROM
        table_count c
        INNER JOIN table_visit v USING(category)
        INNER JOIN table_bounce b USING(category)
        INNER JOIN table_rank r USING(category)
    """
    merged = sqldf(sql, locals()).set_index('category')
    point = """
    SELECT
        category,
        CASE 
            WHEN category_count = 10 THEN 4
            WHEN category_count > 7 THEN 3
            WHEN category_count > 4 THEN 2
            ELSE 1
        END AS point_count,
        CASE
            WHEN avg_category_pages > 10 THEN 4
            WHEN avg_category_pages > 7 THEN 3
            WHEN avg_category_pages > 4 THEN 2
            ELSE 1
        END AS point_visit,
        CASE
            WHEN avg_bounce_rate_in_percent > 50 THEN 4
            WHEN avg_bounce_rate_in_percent > 35 THEN 3
            WHEN avg_bounce_rate_in_percent > 20 THEN 2
            ELSE 1
        END AS point_bounce
    FROM merged
    """
    point_table = sqldf(point, locals())
    total = """
    SELECT
        category,
        point_count,
        point_visit,
        point_bounce,
        (point_count + point_visit + point_bounce) AS TOTAL_POINT
    FROM point_table
    ORDER BY TOTAL_POINT DESC
    """
    result = sqldf(total, locals()).set_index('category')
    return result
