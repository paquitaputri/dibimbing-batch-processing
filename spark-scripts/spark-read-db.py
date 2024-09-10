import pyspark
import os
import json
import argparse

from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, IntegerType, FloatType


dotenv_path = Path('/resources/.env')
load_dotenv(dotenv_path=dotenv_path)


postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('Dibimbing')
        .setMaster('local')
    ))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}


try :
    retail_df = spark.read.jdbc(
        jdbc_url,
        'public.retail',
        properties=jdbc_properties
    )
except Exception as e:
    print("Error load data from PostgreSQL")


retail_df.show(5)
    
# Data Cleaning

# Fill Null with zero

retail_df = retail_df.fillna(0, subset=["quantity", "unitprice"])

# Convert columns to numeric types
retail_df = retail_df.withColumn("quantity", F.col("quantity").cast(IntegerType())).withColumn("unitprice", F.col("unitprice").cast(FloatType()))

# providing summary statistics for numeric columns

retail_df.select("quantity", "unitprice").describe().show()


# 1. Simple Aggregation (TOP 5 Most Popular Items)
most_items = retail_df.groupBy("description").agg(
    F.sum("quantity").alias("total_quantity")
)

most_items = most_items.orderBy("total_quantity", ascending=False).show(5)

# 2. Simple Aggregation (TOP 5 Most Revenue)
most_revenue = retail_df.groupBy("description").agg(
    F.sum(retail_df["quantity"] * retail_df["unitprice"]).alias("revenue")
)

most_revenue = most_revenue.orderBy("revenue", ascending=False).show(5)


# 3. Window Function (Top 5 Most Popular Items Per Country)

# Calculate total quantity per item within each category
retail_total_quantity = retail_df.groupBy("country", "description").agg(
    F.sum("quantity").alias("total_quantity")
)

window_spec = Window.partitionBy("country").orderBy(retail_total_quantity["total_quantity"].desc())

# Add a row number to rank items within each category
retail_ranked = retail_total_quantity.withColumn(
    "Rank", F.row_number().over(window_spec)
)

# Filter to get top 5 items per category
df_top5 = retail_ranked.filter(retail_ranked["Rank"] <= 5).orderBy("country", "Rank")

# Show the result
df_top5.show()

# Insert data to PostgreSQL
df_top5.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "Top_5_Most_Popular_Items_Per_Country")  \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()


# Stop SparkSession
spark.stop()