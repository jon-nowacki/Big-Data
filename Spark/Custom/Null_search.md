```
from pyspark.sql.functions import col, when, count

# Load your table (e.g., Delta, Parquet)
df = spark.read.parquet("dbfs:/your_table_path.parquet")

# Get columns except 'SKU_NBR'
columns_to_check = [c for c in df.columns if c != 'SKU_NBR']

# Count nulls per row, flag rows with at least one null
df_with_nulls = df.select(
    when(
        sum([col(c).isNull().cast("int") for c in columns_to_check]) > 0, 1
    ).otherwise(0).alias("has_null")
)

# Aggregate to get total rows with nulls
null_row_count = df_with_nulls.agg(count(when(col("has_null") == 1, 1)).alias("null_rows")).collect()[0]["null_rows"]

print(f"Number of rows with null values: {null_row_count}"

```
