from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="Intentionally slow PySpark job for Spark UI diagnostics")
    parser.add_argument("--input_path", type=str, required=True, help="Path to input CSV (messy_cars.csv)")
    parser.add_argument("--output_path", type=str, required=True, help="Path to write output parquet")
    return parser.parse_args()


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("SlowJob_DiagnosticDemo")
        # ANTI-PATTERN: only 2 shuffle partitions -> massive partitions after any shuffle
        .config("spark.sql.shuffle.partitions", "2")
        # ANTI-PATTERN: tiny executor memory -> GC pressure + spill to disk
        .config("spark.executor.memory", "512m")
        # ANTI-PATTERN: disable broadcast join -> forces shuffle join on all joins
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_skewed_dataset(spark, input_path):
    """
    Read base CSV (1000 rows) and cross-join with range(2000) to produce ~2M rows.
    Then skew car_make so 80% of rows = 'Toyota'.
    """
    df = spark.read.option("header", "true").csv(input_path)

    # Basic renames so downstream code is readable
    df = (df
          .withColumnRenamed("Car Make", "car_make")
          .withColumnRenamed("price($)", "price_raw")
          .withColumnRenamed("Mileage(km) ", "mileage_raw")
          .withColumnRenamed("YEAR ", "year_raw"))

    df = df.withColumn("price_usd",  F.regexp_replace("price_raw",  r"[^0-9.]", "").cast(FloatType()))
    df = df.withColumn("mileage_km", F.regexp_replace("mileage_raw", r"[^0-9.]", "").cast(FloatType()))
    df = df.withColumn("year",       F.regexp_extract("year_raw",   r"(\d{4})", 1).cast(IntegerType()))

    # Expand: 1000 rows x 2000 copies = ~2,000,000 rows
    multiplier = spark.range(2000).toDF("copy_id")
    df = df.crossJoin(multiplier).drop("copy_id")

    # ANTI-PATTERN (skew): 80% of all rows get make='Toyota'
    df = df.withColumn(
        "car_make",
        F.when(F.rand(seed=42) < 0.8, F.lit("Toyota")).otherwise(F.col("car_make"))
    )

    return df


# ---------------------------------------------------------------------------
# Stage 1 — Data Skew
# What to watch: Stages tab -> task list
# One task processes ~1.6M Toyota rows; all others share ~400k rows
# ---------------------------------------------------------------------------
def stage_data_skew(df):
    print("[STAGE 1] Running skewed groupBy — watch Stages tab for one outlier task")

    skewed_agg = df.groupBy("car_make").agg(
        F.count("*").alias("total_listings"),
        F.avg("price_usd").alias("avg_price"),
        F.sum("mileage_km").alias("total_mileage"),
        F.stddev("price_usd").alias("price_stddev"),
        F.max("mileage_km").alias("max_mileage"),
        F.min("price_usd").alias("min_price")
    )

    # Cache without ever unpersisting (contributes to GC pressure in stage 3)
    skewed_agg.cache()
    count = skewed_agg.count()
    print(f"[STAGE 1] Done — {count} distinct makes")
    return skewed_agg


# ---------------------------------------------------------------------------
# Stage 2 — Shuffle Spike
# What to watch: SQL tab -> Exchange nodes show hundreds of MB shuffle read/write
# ---------------------------------------------------------------------------
def stage_shuffle_spike(df):
    print("[STAGE 2] Running self-join on skewed key — watch SQL tab for Exchange shuffle size")

    # coalesce(1) funnels all 2M rows into a single partition before the join
    df_left = df.coalesce(1).select(
        F.col("car_make"),
        F.col("price_usd"),
        F.col("mileage_km"),
        F.col("year")
    )
    df_right = df.select(
        F.col("car_make").alias("make_r"),
        F.col("price_usd").alias("price_r")
    )

    # Self-join on the skewed key with no broadcast hint:
    # Toyota partition joins Toyota with Toyota -> O(n^2) shuffle for that key
    joined = df_left.join(df_right, df_left["car_make"] == df_right["make_r"], "inner")
    joined.cache()
    count = joined.count()
    print(f"[STAGE 2] Done — {count} joined rows")
    return joined


# ---------------------------------------------------------------------------
# Stage 3 — GC Pressure
# What to watch: Executors tab -> GC Time column (high % of task time)
# ---------------------------------------------------------------------------
def stage_gc_pressure(df):
    print("[STAGE 3] Accumulating cached DataFrames — watch Executors tab for GC time")

    cached = []
    for i in range(5):
        # Each iteration derives a new column and caches without unpersisting
        temp = df.withColumn(f"derived_{i}", F.col("price_usd") * i + F.col("mileage_km"))
        temp.cache()
        temp.count()   # Materialise into executor memory
        cached.append(temp)

    # Collect a large slice to the driver — pressures both executor and driver heap
    sample = df.limit(50000).collect()
    print(f"[STAGE 3] Done — collected {len(sample)} rows to driver; {len(cached)} DFs still cached")
    return cached


# ---------------------------------------------------------------------------
# Stage 4 — Spill to Disk
# What to watch: Stages -> task metrics -> "Shuffle Spill (Memory/Disk)" columns
# ---------------------------------------------------------------------------
def stage_spill_to_disk(df):
    print("[STAGE 4] Running coalesce(1) + groupBy with 2 shuffle partitions — watch task spill metrics")

    spill_df = (
        df.coalesce(1)                              # 1 giant input partition
          .groupBy("car_make", "year")              # shuffle into only 2 output partitions
          .agg(
              F.count("*").alias("cnt"),
              F.avg("price_usd").alias("avg_price")
          )
          .orderBy(F.col("cnt").desc())             # second sort shuffle
    )

    spill_df.cache()
    count = spill_df.count()
    print(f"[STAGE 4] Done — {count} make/year combinations")
    return spill_df


def write_results(df, output_path):
    # Write aggregated result as a single file (no parallelism, slow write)
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    print(f"[WRITE] Output written to {output_path}")


def main():
    args = parse_args()
    spark = create_spark_session()

    print("=" * 60)
    print("  Slow Job: Spark UI Diagnostic Demo")
    print("  Anti-patterns: skew | shuffle | GC | spill")
    print("=" * 60)

    df = build_skewed_dataset(spark, args.input_path)

    # Each stage is a separate Spark action — they appear as distinct stages in Spark UI
    skewed_agg  = stage_data_skew(df)
    _joined     = stage_shuffle_spike(df)
    _cached     = stage_gc_pressure(df)
    spill_df    = stage_spill_to_disk(df)

    write_results(skewed_agg, args.output_path)

    spark.stop()
    print("=" * 60)
    print("  Done. Open Spark History Server to review the stages.")
    print("=" * 60)


if __name__ == "__main__":
    main()
