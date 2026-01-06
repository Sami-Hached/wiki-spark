from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.types import StructType, StructField, StringType, LongType

log_folder = "/Users/sami.hached/Documents/wiki-dataset/pageviews-2020-01/01/"
spark = SparkSession.builder.appName("task_01").getOrCreate()

pageview_schema = StructType([
    StructField("domain_code", StringType(), True),
    StructField("page_title", StringType(), True),
    StructField("count_views", LongType(), True),
    StructField("total_response_size", LongType(), True),
])

log_data = spark.read.csv(log_folder, sep=" ", schema=pageview_schema)
log_data.createOrReplaceTempView("log_data")
spark.sql("""
    SELECT page_title, SUM(count_views) AS daily_views
    FROM log_data
    WHERE
        domain_code = 'en' AND
        page_title NOT IN ('-', 'Main_Page', 'Special:Search')
    GROUP BY page_title
    ORDER BY daily_views DESC LIMIT 20
""").write.csv(f"top_20_articles_sql.csv")

# Another approach
(
    log_data
    .filter(log_data.domain_code == "en")
    .filter(~log_data.page_title.isin(["Main_Page", "-", "Special:Search"]))
    .groupBy("page_title")
    .agg(sum("count_views").alias("daily_views"))
    .orderBy("daily_views", ascending=False)
    .limit(20)
    .write.csv(f"top_20_articles_python.csv")
 )

spark.stop()