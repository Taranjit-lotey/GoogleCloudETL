from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, FloatType
from datetime import datetime,timedelta
import random
import argparse

#Get the parameters from inline

def parse_args():
    parser = argparse.ArgumentParser(description="Transform messy car data")
    parser.add_argument("--input_path", type=str, required=True, help="Path to input messy car data")
    parser.add_argument("--output_path", type=str, required=True, help="Path to save cleaned car data")
    parser.add_argument("--watermark_date",required=False, type=str, help="Watermark date for filtering old records (YYYY-MM-DD)")
    return parser.parse_args()

#Start the spark session

def create_spark_session():
    spark = SparkSession.builder.appName("CarDataTransformation").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

#Create schema for the data
def clean_column_schema(df):
    rename = {
        "Car Make":         "car_make",
        "model_NAME":       "model_name",
        "YEAR ":            "year",
        "price($)":         "price_usd",
        "Mileage(km) ":     "mileage_km",
        "colour/Color":     "color",
        " fuel_Type":       "fuel_type",
        "No. of Owners":    "num_owners"
    }

    for old_name, new_name in rename.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df

#Read the CSV file

def read_data(spark, input_path):
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(input_path)
    return df

#Change Data types and clean the data

def clean_data(df):
    df = clean_column_schema(df)

    # Clean and convert year
    df = df.withColumn("year", F.regexp_extract("year", r"(\d{4})", 1).cast(IntegerType()))

    # Clean and convert price
    df = df.withColumn("price_usd", F.regexp_replace("price_usd", r"[^0-9.]", "").cast(FloatType()))

    # Clean and convert mileage
    df = df.withColumn("mileage_km", F.regexp_replace("mileage_km", r"[^0-9.]", "").cast(FloatType()))

    # Clean and convert num_owners
    df = df.withColumn("num_owners", F.regexp_extract("num_owners", r"(\d+)", 1).cast(IntegerType()))

    return df

#Find the total miles per year for each car make and model
def calculate_miles_per_year(df):
    current_year = datetime.now().year
    df = df.withColumn("miles_per_year", F.col("mileage_km") / (current_year - F.col("year")))
    return df

#Standardize column values, drop nulls, filter data
def transform_data(df):
    # Standardize color values
    df = df.withColumn("color", F.lower(F.col("color")))

    # Drop rows with null values in critical columns
    df = df.dropna(subset=["car_make", "model_name", "year", "price_usd", "mileage_km"])

    # Filter out records with unrealistic year or price
    df = df.filter((F.col("year") >= 1990) & (F.col("year") <= datetime.now().year))
    df = df.filter((F.col("price_usd") >= 500) & (F.col("price_usd") <= 200000))

    df= df.withColumn("total_miles_per_year", F.col("mileage_km") / (datetime.now().year - F.col("year")))

    #audit column
    df = df.withColumn("audit_timestamp", F.current_timestamp())
    return df

#watermarking old records --this comes directly from the parameters passed in inline 
def apply_watermark(df, watermark_date):
    #create date using year of car
    df = df.withColumn("list_date", F.to_date(F.concat(F.col("year"), F.lit("-01-01"))))

    if watermark_date:
        watermark_date = datetime.strptime(watermark_date, "%Y-%m-%d")
        df = df.filter(F.col("audit_timestamp") >= F.lit(watermark_date))
    return df

#write the cleaned data back to GCS in parquet format
def write_data(df, output_path):
    #this partition is creating a folder for each year and writing the data in those folders, this is a common practice to optimize query performance when reading the data later
    df.write.mode("overwrite").partitionBy("year").parquet(output_path)    
    print(f"Written to GCS: {output_path}")
    print(f"Processed row count: {df.count()}")

#loading to bigQ directly
#will change to do using GCSToBigQueryOperator in Airflow DAG, but keeping this here for reference and testing purposes
def load_to_bigquery(spark, output_path, bq_dataset, bq_table):
    df = spark.read.parquet(output_path)
    
    df.write \
        .format("bigquery") \
        .option("table", f"{bq_dataset}.{bq_table}") \
        .option("temporaryGcsBucket", "etl-migration-1-data") \
        .option("partitionField", "listing_date") \
        .option("partitionType", "YEAR") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_TRUNCATE") \
        .option("clusteredFields", "car_make,fuel_type") \
        .mode("overwrite") \
        .save()
    
    print(f"Loaded into BigQuery: {bq_dataset}.{bq_table}")


def main():
    args = parse_args()
    
    spark = create_spark_session()
    
    # 1. Read
    df = read_data(spark, args.input_path)

    # 2. Clean column names + cast types
    df = clean_data(df)
    
    # 4. Transform
    df = transform_data(df)
    
    # 5. Apply watermark
    df = apply_watermark(df, args.watermark_date)
    
    # 6. Write to GCS
    write_data(df, args.output_path)

    # 7. Load into BigQuery
    load_to_bigquery(
        spark,
        args.output_path,
        bq_dataset="car_listings_dataset",
        bq_table="car_listings"
    )
    
    spark.stop()
    print("Pipeline complete.")

if __name__ == "__main__":
    main()

