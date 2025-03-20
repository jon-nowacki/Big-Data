'''
import polars as pl

# Assuming your table is in a Databricks table or file (e.g., Parquet)
# Replace 'your_table_path' with actual path or table name
df = pl.read_parquet("dbfs:/your_table_path.parquet")  # or pl.read_csv, etc.

# Get all columns except 'SKU_NBR'
columns_to_check = [col for col in df.columns if col != 'SKU_NBR']

# Check for nulls in all other columns, row-wise
# `is_null()` returns True for nulls, `any_horizontal` checks across columns per row
rows_with_nulls = df.select(
    pl.any_horizontal([pl.col(col).is_null() for col in columns_to_check]).alias("has_null")
)

# Count rows with at least one null
null_row_count = rows_with_nulls.filter(pl.col("has_null")).height

print(f"Number of rows with null values: {null_row_count}")
'''
