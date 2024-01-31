# Run this code on a cluster with Spark installed!

# We used the following command to run the script:
# spark-submit --packages com.databricks:spark-xml_2.12:0.17.0 project.py 2> /dev/null

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analysing Trends in Software Development").getOrCreate()

# Read the XML file and transform the data
df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .load('/user/s2367114/stackoverflow/Posts.xml')
    .select(
        F.explode(
            F.split(
                F.regexp_replace(F.regexp_replace(F.col("_Tags"), "^<", ""), ">$", ""), "><"
            )
        ).alias("tags"),
        F.date_trunc('week', F.col("_CreationDate")).alias('week'),
    )
)

df = df.groupBy("tags", "week").count()

df = df.groupBy("tags").agg(F.collect_list(F.struct("week", "count")).alias("week_counts"))

# Writes the output to a parquet file on the HDFS
df.write.mode("overwrite").parquet("tags_output.parquet")