'''
--conf spark.jars=s3://<s3_bucket_name>/jars/iceberg-aws-bundle-1.10.0.jar,s3://<s3_bucket_name>/jars/iceberg-spark-runtime-3.5_2.12-1.10.0.jar
'''

from pyspark.sql import SparkSession

# Import additional libraries
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
from pyspark.sql.types import *
from datetime import datetime
import sys

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("IcebergIntegration") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://<s3_bucket_name>/iceberg/") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

query = f"""
CALL glue_catalog.system.rewrite_table_path(
  table => 'glue_catalog.iceberg.sampledataicebergtable',
  source_prefix => 's3://<s3_bucket_name>/<s3_file_path_to_iceberg_metadata_of_table_to_be_migrated>', # The existing prefix to be replaced. ex. 's3://iceberg-s3-biob6mn0znrc/iceberg/iceberg.db/sampledataicebergtable',
  target_prefix => 's3://<s3_bucket_name>/<s3_file_path_to_iceberg_metadata_where_the_table_will_be_migrated_to>', # The replacement prefix for source_prefix. ex. 's3://iceberg-s3-biob6mn0znrc-copy-to/iceberg/iceberg.db/sampledataicebergtable',
  staging_location => 's3://<s3_bucket_name>/<path_the_updated_metadata_to_be_written_to>')
"""

spark.sql(query)
