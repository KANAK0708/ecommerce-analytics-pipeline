"""
Spark Session Utility for local and Glue PySpark execution.
"""

from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "EcommerceMedallionETL") -> SparkSession:
    """
    Creates or retrieves an active SparkSession with optimized configs
    for Parquet compression and local/cluster execution.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
