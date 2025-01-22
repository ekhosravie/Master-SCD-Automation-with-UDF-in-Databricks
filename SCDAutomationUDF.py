Start
#   |
#   |---> Parameters: source_df, delta_table_path, primary_keys, update_columns, effective_date_col, end_date_col, current_flag_col
#   |
#   |---> Check if Delta table exists at delta_table_path
#          |
#          |---> If Delta table exists
#          |         |
#          |         |---> Load Delta table as target_df
#          |         |
#          |         |---> If audit_columns is enabled:
#          |         |         Add created_at, updated_at to source_df
#          |         |
#          |         |---> Add effective_date_col to source_df with current date
#          |         |
#          |         |---> Define merge_condition based on primary_keys
#          |         |
#          |         |---> Define update_condition to detect changes in update_columns
#          |         |
#          |         |---> Perform Delta MERGE
#          |         |         |
#          |         |         |---> WHEN MATCHED AND update_condition IS TRUE:
#          |         |         |         Update target_df:
#          |         |         |         - Set update_columns to source values
#          |         |         |         - Set end_date_col to current date
#          |         |         |         - Set current_flag_col to False
#          |         |         |         - Update audit columns (updated_at)
#          |         |
#          |         |         |---> WHEN NOT MATCHED:
#          |         |         |         Insert new records:
#          |         |         |         - Include all source_df columns
#          |         |         |         - Set end_date_col to NULL
#          |         |         |         - Set current_flag_col to True
#          |         |         |         - Include audit columns (created_at)
#          |         |
#          |         |---> Delta MERGE execution completed
#          |
#          |---> If Delta table does NOT exist
#                   |
#                   |---> If audit_columns is enabled:
#                   |         Add created_at, updated_at to source_df
#                   |
#                   |---> Add effective_date_col to source_df with current date
#                   |---> Add end_date_col to source_df as NULL
#                   |---> Add current_flag_col to source_df as True
#                   |---> Write source_df as a new Delta table at delta_table_path
#   |
#   |---> End



from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

def scd_handler_delta(
    spark,
    source_df,
    delta_table_path,
    primary_keys,
    update_columns=None,
    scd_type=2,
    effective_date_col="effective_date",
    end_date_col="end_date",
    is_current_col="is_current",
    prev_value_columns=None
):
    """
    Handles Slowly Changing Dimensions (SCD) for Delta tables.

    :param spark: Spark session
    :param source_df: Source DataFrame
    :param delta_table_path: Path to the Delta table
    :param primary_keys: List of primary keys for identifying records
    :param update_columns: List of columns to update in SCD Type 2
    :param scd_type: Type of SCD (1, 2, or 3)
    :param effective_date_col: Column for the effective date (default 'effective_date')
    :param end_date_col: Column for the end date (default 'end_date')
    :param is_current_col: Column indicating if the record is current (default 'is_current')
    :param prev_value_columns: List of columns for SCD Type 3 (previous value columns)
    """
    update_columns = update_columns or []
    prev_value_columns = prev_value_columns or []

    try:
        # Check if Delta table exists
        if not DeltaTable.isDeltaTable(spark, delta_table_path):
            # Add required SCD columns for new tables
            if scd_type == 2:
                source_df = source_df.withColumn(effective_date_col, F.current_timestamp()) \
                                     .withColumn(end_date_col, F.lit(None).cast(TimestampType())) \
                                     .withColumn(is_current_col, F.lit(True))
            elif scd_type == 3:
                for col in prev_value_columns:
                    source_df = source_df.withColumn(f"prev_{col}", F.lit(None).cast(StringType()))

            source_df.write.format("delta").mode("overwrite").save(delta_table_path)
            return f"Delta table created at {delta_table_path}."

        delta_table = DeltaTable.forPath(spark, delta_table_path)

        # Generate merge condition
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])

        if scd_type == 1:
            # SCD Type 1: Overwrite matching records and insert new records
            delta_table.alias("target").merge(
                source_df.alias("source"),
                merge_condition
            ).whenMatchedUpdate(
                set={col: f"source.{col}" for col in update_columns}
            ).whenNotMatchedInsert(
                values={col: f"source.{col}" for col in source_df.columns}
            ).execute()

        elif scd_type == 2:
            # SCD Type 2: Keep history of changes
            source_df = source_df.withColumn(effective_date_col, F.current_timestamp())
            delta_table.alias("target").merge(
                source_df.alias("source"), merge_condition
            ).whenMatchedUpdate(
                condition=f"target.{is_current_col} = True AND (" + " OR ".join([f"target.{col} != source.{col}" for col in update_columns]) + ")",
                set={
                    **{col: f"source.{col}" for col in update_columns},
                    end_date_col: F.current_timestamp(),
                    is_current_col: F.lit(False)
                }
            ).whenNotMatchedInsert(
                values={
                    **{col: f"source.{col}" for col in source_df.columns},
                    effective_date_col: F.current_timestamp(),
                    end_date_col: F.lit(None).cast(TimestampType()),
                    is_current_col: F.lit(True)
                }
            ).execute()

        elif scd_type == 3:
            # SCD Type 3: Keep current and previous values for specific columns
            for col in prev_value_columns:
                if f"prev_{col}" not in source_df.columns:
                    source_df = source_df.withColumn(f"prev_{col}", F.lit(None).cast(StringType()))

            delta_table.alias("target").merge(
                source_df.alias("source"), merge_condition
            ).whenMatchedUpdate(
                condition=" OR ".join([f"target.{col} != source.{col}" for col in update_columns]),
                set={
                    **{col: f"source.{col}" for col in update_columns},
                    **{f"prev_{col}": f"target.{col}" for col in prev_value_columns}
                }
            ).whenNotMatchedInsert(
                values={col: f"source.{col}" for col in source_df.columns}
            ).execute()

        else:
            return "Invalid SCD type specified. Use 1, 2, or 3."

        updated_df = spark.read.format("delta").load(delta_table_path)
        return updated_df

    except Exception as e:
        raise Exception(f"Error in SCD handler: {str(e)}")



# Initialize Delta tables with initial data for testing

# Use absolute paths for the Delta tables
delta_table_path_scd1 = "/dbfs/tmp/delta_table_scd1"
delta_table_path_scd2 = "/dbfs/tmp/delta_table_scd2"
delta_table_path_scd3 = "/dbfs/tmp/delta_table_scd3"

# SCD Type 1 - Simple overwrite with no history
schema_scd1 = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

# Initial data for SCD Type 1
data_initial_scd1 = [
    (1, "Alice", 30, "New York"),
    (2, "Bob", 35, "Los Angeles"),
    (3, "Charlie", 40, "Chicago")
]

target_df_scd1 = spark.createDataFrame(data_initial_scd1, schema_scd1)
target_df_scd1.write.format("delta").mode("overwrite").save(delta_table_path_scd1)

# New source data for SCD Type 1
data_source_scd1 = [
    (1, "Alice", 32, "Boston"),  # Updated age and city
    (4, "David", 28, "San Francisco")  # New record
]
source_df_scd1 = spark.createDataFrame(data_source_scd1, schema_scd1)

# SCD Type 2 - Include effective_date, end_date, is_current columns
schema_scd2 = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("effective_date", TimestampType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("is_current", BooleanType(), True)
])

# Initial data for SCD Type 2
data_initial_scd2 = [
    (1, "Alice", 30, "New York", None, None, True),
    (2, "Bob", 35, "Los Angeles", None, None, True),
    (3, "Charlie", 40, "Chicago", None, None, True)
]

target_df_scd2 = spark.createDataFrame(data_initial_scd2, schema_scd2)
target_df_scd2.write.format("delta").mode("overwrite").save(delta_table_path_scd2)

# New source data for SCD Type 2
data_source_scd2 = [
    (1, "Alice", 32, "Boston", None, None, True),  # Updated age and city
    (4, "David", 28, "San Francisco", None, None, True)  # New record
]
source_df_scd2 = spark.createDataFrame(data_source_scd2, schema_scd2)

# SCD Type 3 - Track previous city values
schema_scd3 = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("prev_city", StringType(), True)
])

# Initial data for SCD Type 3
data_initial_scd3 = [
    (1, "Alice", 30, "New York", None),
    (2, "Bob", 35, "Los Angeles", None),
    (3, "Charlie", 40, "Chicago", None)
]

target_df_scd3 = spark.createDataFrame(data_initial_scd3, schema_scd3)
target_df_scd3.write.format("delta").mode("overwrite").save(delta_table_path_scd3)

# New source data for SCD Type 3
data_source_scd3 = [
    (1, "Alice", 32, "Boston", None),  # Updated city
    (4, "David", 28, "San Francisco", None)  # New record
]
source_df_scd3 = spark.createDataFrame(data_source_scd3, schema_scd3)



print("Delta tables initialized and source data prepared for testing SCD Types 1, 2, and 3.")

# Test SCD Type 1
print("\nTesting SCD Type 1...")
result_scd1 = scd_handler_delta(
    spark=spark,
    source_df=source_df_scd1,
    delta_table_path=delta_table_path_scd1,
    primary_keys=["id"],
    update_columns=["name", "age", "city"],
    scd_type=1
)
result_scd1.show()

# Test SCD Type 2
print("\nTesting SCD Type 2...")
result_scd2 = scd_handler_delta(
    spark=spark,
    source_df=source_df_scd2,
    delta_table_path=delta_table_path_scd2,
    primary_keys=["id"],
    update_columns=["name", "age", "city"],
    scd_type=2,
    effective_date_col="effective_date",
    end_date_col="end_date",
    is_current_col="is_current"
)
result_scd2.show()

# Test SCD Type 3
print("\nTesting SCD Type 3...")
result_scd3 = scd_handler_delta(
    spark=spark,
    source_df=source_df_scd3,
    delta_table_path=delta_table_path_scd3,
    primary_keys=["id"],
    update_columns=["name", "age", "city"],
    scd_type=3,
    prev_value_columns=["city"]  # Keep track of previous city
)
result_scd3.show()


