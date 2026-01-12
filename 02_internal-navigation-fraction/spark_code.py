from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

pageviews_folder = "/Users/sami.hached/Documents/wiki-dataset/pageviews-2020-01/"
click_file = "/Users/sami.hached/Documents/wiki-dataset/clickstream-enwiki-2020-01.tsv.gz"
spark = SparkSession.builder.appName("task_02").getOrCreate()

pageview_schema = StructType([
    StructField("domain_code", StringType(), True),
    StructField("page_title", StringType(), True),
    StructField("count_views", LongType(), True),
    StructField("total_response_size", LongType(), True),
])

clicks_schema = StructType([
    StructField("prev", StringType(), False),
    StructField("curr", StringType(), False),
    StructField("type", StringType(), False),
    StructField("count", LongType(), False),
])

pageview_data = spark.read.csv(pageviews_folder, sep=" ", schema=pageview_schema)
pageview_data.createOrReplaceTempView("pageview_data")

clicks_data = spark.read.csv(click_file, sep="\t", schema=clicks_schema)
clicks_data.createOrReplaceTempView("clicks_data")

pageview_data.groupby()
# filter for english wikipedia
pageview_data_clean = (
    pageview_data
    .drop("total_response_size")
    .filter(pageview_data.domain_code == "en")
    .groupby("page_title")
    .agg(sum("count_views").alias("monthly_views"))
)

# link clicks means internal wikipage navigation
clicks_data_clean = (
    clicks_data
    .filter(clicks_data.type == "link")
    .groupby("curr")
    .agg(sum("count").alias("internal_clicks"))
)

join_data = (
    pageview_data_clean
    .join(
        clicks_data_clean,
        pageview_data_clean.page_title == clicks_data_clean.curr, "inner"
    )
    .select("page_title", "monthly_views", "internal_clicks")
)

internal_fraction = (
    join_data
    .select(
        col("page_title"),
        (col("internal_clicks") / col("monthly_views")).alias("internal_fraction")
    )
    .orderBy("internal_fraction", ascending=False)
).write.csv(f"top_20_articles_from_internal_clicks.csv")

print("Done")