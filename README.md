# Master-SCD-Automation-with-UDF-in-Databricks

# **Automated SCD Handling with PySpark in Databricks**

## **Introduction**  
This repository provides a reusable and modular **PySpark function** to automate the handling of **Slowly Changing Dimensions (SCD) Types 1, 2, and 3** in Databricks Delta Tables. The function uses **User-Defined Functions (UDF)** to simplify the process, reduce complexity, and improve scalability for large datasets.

---

## **Purpose of This Function**  

Handling Slowly Changing Dimensions (SCDs) is a critical aspect of data engineering, especially in maintaining historical data and ensuring data accuracy.  

Manually implementing SCDs is:  
- **Time-consuming**: Comparing records, tracking changes, and updating historical data for large datasets takes considerable effort.  
- **Error-prone**: Manual handling can lead to data inconsistencies and lost historical records.  
- **Complex**: Each SCD type requires its logic, making manual implementations repetitive and inefficient.  

This **automated function** addresses these challenges by:  
1. Providing a **scalable solution** for large datasets.  
2. Leveraging a **UDF** for reusability and efficiency.  
3. Supporting **SCD Types 1, 2, and 3** with minimal setup.  

---

## **Features**  

1. **Supports Multiple SCD Types**  
   - **Type 1**: Overwrites old records without keeping history.  
   - **Type 2**: Tracks historical changes with `effective_date` and `end_date` fields.  
   - **Type 3**: Maintains both current and previous values for certain fields.  

2. **Optimizations**  
   - **Column Handling**: Simplified logic for adding and updating columns.  
   - **Merge Conditions**: Dynamically constructed based on input parameters.  
   - **Audit Columns**: Includes `created_at` and `updated_at` for traceability.  
   - **Schema Validations**: Ensures source and target schemas are compatible.  

3. **Customizations**  
   - Supports custom update conditions for SCD Types 2 and 3.  
   - Flexible options for logging, auditing, and concurrency.  

---

## **Parameters**  

- `source_df`: The incoming DataFrame with updates.  
- `delta_table_path`: Path to the Delta Table for storing dimensions.  
- `primary_keys`: List of columns used as unique identifiers.  
- `update_columns`: List of columns to track for changes.  
- `effective_date_col`: Name of the column to store the effective date for SCD Type 2.  
- `end_date_col`: Name of the column to store the end date for SCD Type 2.  
- `current_flag_col`: Name of the column to indicate the current record for SCD Type 2.  
- `audit_columns`: Boolean flag to include audit columns (`created_at`, `updated_at`).  

---

## **How It Works**  

1. **Delta Table Check**  
   - If the Delta table exists:  
     - Load it as `target_df` and perform a Delta MERGE.  
   - If it doesn’t exist:  
     - Initialize the Delta table by writing the `source_df`.  

2. **Delta MERGE Logic**  
   - **When Matched**: Update records if changes are detected in the `update_columns`.  
   - **When Not Matched**: Insert new records with SCD-specific columns.  

3. **Column Initialization**  
   - Automatically adds necessary columns (`effective_date`, `end_date`, `current_flag`, etc.).  

4. **Audit Tracking**  
   - Includes `created_at` and `updated_at` timestamps for better traceability.  

---

## **Usage**  

### **1. Install Prerequisites**  
Ensure you have access to **Databricks** and the required **PySpark libraries**.  

### **2. Load the Function**  
Import the function into your notebook or Databricks environment:  

```python
from your_module import handle_scd
```

### **3. Configure Parameters**  

```python
handle_scd(
    source_df=source_data,
    delta_table_path="/mnt/delta/scd_table",
    primary_keys=["id"],
    update_columns=["name", "age", "city"],
    effective_date_col="effective_date",
    end_date_col="end_date",
    current_flag_col="is_current",
    audit_columns=True
)
```

### **4. Test with SCD Types**  
Run the function with datasets representing updates for SCD Types 1, 2, and 3 to validate the results.

---

## **Results**  

1. **SCD Type 1**:  
   Overwrites old records with new values.  

2. **SCD Type 2**:  
   Creates new records with `effective_date`, `end_date`, and `current_flag` fields.  

3. **SCD Type 3**:  
   Maintains a snapshot of the previous value alongside the current one.  

---

## **Code Sample Output**  

```plaintext
Delta tables initialized and source data prepared for testing SCD Types 1, 2, and 3.

Testing SCD Type 1...
+---+-------+---+-------------+
| id|   name|age|         city|
+---+-------+---+-------------+
|  4|  David| 28|San Francisco|
|  2|    Bob| 35|  Los Angeles|
|  3|Charlie| 40|      Chicago|
|  1|  Alice| 32|       Boston|
+---+-------+---+-------------+


Testing SCD Type 2...
+---+-------+---+-------------+--------------------+--------------------+----------+
| id|   name|age|         city|      effective_date|            end_date|is_current|
+---+-------+---+-------------+--------------------+--------------------+----------+
|  4|  David| 28|San Francisco|2025-01-22 03:28:...|                null|      true|
|  1|  Alice| 32|       Boston|                null|2025-01-22 03:28:...|     false|
|  2|    Bob| 35|  Los Angeles|                null|                null|      true|
|  3|Charlie| 40|      Chicago|                null|                null|      true|
+---+-------+---+-------------+--------------------+--------------------+----------+


Testing SCD Type 3...
+---+-------+---+-------------+---------+
| id|   name|age|         city|prev_city|
+---+-------+---+-------------+---------+
|  1|  Alice| 32|       Boston| New York|
|  4|  David| 28|San Francisco|     null|
|  2|    Bob| 35|  Los Angeles|     null|
|  3|Charlie| 40|      Chicago|     null|
+---+-------+---+-------------+---------+
...
```

---

## **Contributing**  
Contributions are welcome! Feel free to submit a pull request or open an issue for feature requests or bug fixes.

---

## **License**  
This project is licensed under the MIT License.

---
