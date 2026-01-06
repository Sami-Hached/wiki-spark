from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import SparkSession

log_file = "/Users/sami.hached/Documents/wiki-dataset/pageviews-20200101-000000.gz"
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()


schema = StructType([
    StructField("domain_code", StringType(), True),
    StructField("page_title", StringType(), True),
    StructField("count_views", LongType(), True),
    StructField("total_response_size", LongType(), True),
])

log_data = spark.read.csv(log_file, sep=" ", schema=schema)
log_data.createOrReplaceTempView("log_data")

numAs = log_data.filter(log_data.page_title.contains("a")).count()
numBs = log_data.filter(log_data.page_title.contains("b")).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.sql("""
    SELECT domain_code, SUM(count_views) AS total_views
    FROM log_data
    GROUP BY domain_code
    ORDER BY total_views DESC
""").show()

spark.stop()