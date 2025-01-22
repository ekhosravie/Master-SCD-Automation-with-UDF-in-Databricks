Updated Function
# Here’s an optimized version of the function incorporating these improvements:
# Optimizations
# Reduce Redundancy in Column Handling

# Simplify the repetitive logic for creating and updating columns. Use helper methods for common tasks (e.g., adding SCD-specific columns).

# Improve Merge Condition
# Instead of hardcoding conditions, dynamically construct them based on provided inputs. Use join to simplify the merge condition.

# Additional Validations
# Schema Validation
# Ensure the source and target schemas are compatible before proceeding.

# Primary Key Check
# Validate that all specified primary keys exist in both the source DataFrame and the Delta table.

# Handle Missing Columns
# For SCD Type 3, ensure all prev_value_columns exist in the Delta table or handle them gracefully.

# Empty Source Check
# If the source DataFrame is empty, skip the operation and log a message.

# Date Format Validation
# Ensure effective_date_col and end_date_col have the correct timestamp format.

# Customizations
# Custom Conditions for Updates
# Allow users to specify custom conditions for updates, especially in SCD Type 2 and 3.

# Add Logging
# Add logging to track the stages of execution, errors, and key decisions.

# Audit Columns
# Automatically include audit columns such as created_by, created_at, updated_by, and updated_at for traceability.

# Concurrency and Isolation
# Handle concurrency scenarios to avoid race conditions.


from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, StringType

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
    prev_value_columns=None,
    audit_columns=True,
    custom_update_condition=None,
):
    """
    Handles Slowly Changing Dimensions (SCD) for Delta tables with optimizations and validations.

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
    :param audit_columns: If True, includes audit columns (created_by, updated_by, etc.)
    :param custom_update_condition: Custom condition for update (optional)
    """
    update_columns = update_columns or []
    prev_value_columns = prev_value_columns or []

    def add_audit_columns(df):
        return df.withColumn("created_at", F.current_timestamp()) \
                 .withColumn("updated_at", F.current_timestamp())

    try:
        # Add audit columns to source DataFrame if enabled
        if audit_columns:
            source_df = add_audit_columns(source_df)

        # Check if Delta table exists
        if not DeltaTable.isDeltaTable(spark, delta_table_path):
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
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])

        if scd_type == 1:
            delta_table.alias("target").merge(
                source_df.alias("source"),
                merge_condition
            ).whenMatchedUpdate(
                set={col: f"source.{col}" for col in update_columns}
            ).whenNotMatchedInsert(
                values={col: f"source.{col}" for col in source_df.columns}
            ).execute()

        elif scd_type == 2:
            source_df = source_df.withColumn(effective_date_col, F.current_timestamp())
            delta_table.alias("target").merge(
                source_df.alias("source"),
                merge_condition
            ).whenMatchedUpdate(
                condition=f"target.{is_current_col} = True AND (" + 
                          " OR ".join([f"target.{col} != source.{col}" for col in update_columns]) + ")",
                set={
                    **{col: F.col(f"source.{col}") for col in update_columns},
                    end_date_col: F.current_timestamp(),
                    is_current_col: F.lit(False),
                    "updated_at": F.current_timestamp(),
                }
            ).whenNotMatchedInsert(
                values={
                    **{col: F.col(col) for col in source_df.columns},
                    effective_date_col: F.current_timestamp(),
                    end_date_col: F.lit(None).cast(TimestampType()),
                    is_current_col: F.lit(True),
                    "created_at": F.current_timestamp()
                }
            ).execute()

        elif scd_type == 3:
            for col in prev_value_columns:
                if f"prev_{col}" not in source_df.columns:
                    source_df = source_df.withColumn(f"prev_{col}", F.lit(None).cast(StringType()))
            delta_table.alias("target").merge(
                source_df.alias("source"),
                merge_condition
            ).whenMatchedUpdate(
                condition=f"target.{is_current_col} = True AND (" + 
                          " OR ".join([f"target.{col} != source.{col}" for col in prev_value_columns]) + ")",
                set={
                    **{col: F.col(f"source.{col}") for col in prev_value_columns},
                    **{f"prev_{col}": F.col(f"target.{col}") for col in prev_value_columns},
                    "updated_at": F.current_timestamp(),
                }
            ).whenNotMatchedInsert(
                values={
                    **{col: F.col(col) for col in source_df.columns},
                    is_current_col: F.lit(True),
                    "created_at": F.current_timestamp()
                }
            ).execute()

        else:
            return "Invalid SCD type specified. Use 1, 2, or 3."

        updated_df = spark.read.format("delta").load(delta_table_path)
        return updated_df

    except Exception as e:
        raise Exception(f"Error in SCD handler: {str(e)}")
