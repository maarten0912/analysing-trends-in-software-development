# Run this code on a cluster with Spark installed!

# We used the following command to run the script:
# spark-submit --packages com.databricks:spark-xml_2.12:0.17.0 project.py 2> /dev/null

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analysing Trends in Software Development").getOrCreate()

df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .load('/user/s2367114/stackoverflow/Posts.xml')
    .select(
        F.date_trunc('week', F.col("_CreationDate")).alias('week'),
    )
)

df = df.groupBy("week").count()

df.write.mode("overwrite").parquet("total_posts_per_week.parquet")
