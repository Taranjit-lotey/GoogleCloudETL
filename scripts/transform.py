from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, FloatType
from datetime import datetime,timedelta
import random

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

    spark.sparkConntext.setLogLevel("WARN")
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
        "fuel_Type":        "fuel_type",
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

    df= df.withColumn("total miles_per_year", F.col("mileage_km") / (datetime.now().year - F.col("year")))

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

