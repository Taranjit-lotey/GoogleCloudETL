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

