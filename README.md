# to_date-and-date_format
# Explanation of `to_date` and `date_format` in Apache Spark

The `to_date` and `date_format` functions in Apache Spark serve different purposes when dealing with date and time data. Below is a detailed comparison and examples demonstrating their differences.

---

## Difference Between `to_date` and `date_format`

| **Function**   | **Purpose**                                                                       | **Input Type**                 | **Output Type**    | **Example Output**                                   |
|-----------------|-----------------------------------------------------------------------------------|--------------------------------|--------------------|-----------------------------------------------------|
| `to_date`      | Converts a string or timestamp into a `DateType`.                                | String or Timestamp            | `DateType`         | Converts `"2025-01-09"` → `2025-01-09`                   |
| `date_format`  | Formats a `DateType` or `TimestampType` column into a string with a specific format. | `DateType` or `TimestampType` | String             | Formats `2025-01-09` → `"Jan 09, 2025"`                 |

---

## Example Workflow with Input and Output

### Input Table:

| date_string    | timestamp            |
|----------------|----------------------|
| `2025-01-09`  | `2025-01-09 12:30:45`|
| `01-09-2025`  | `2025-01-09 00:00:00`|
| `2025/01/09`  | `2025-01-09 06:15:00`|
| `20250109`    | `2025-01-09 23:59:59`|

### Processing with `to_date`:

The `to_date` function standardizes various date-like strings into a consistent `DateType` format.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

# Create SparkSession
spark = SparkSession.builder.appName("ToDate Example").getOrCreate()

# Sample DataFrame
data = [
    ("2025-01-09",),
    ("01-09-2025",),
    ("2025/01/09",),
    ("20250109",)
]
columns = ["date_string"]
df = spark.createDataFrame(data, columns)

# Apply to_date with specific format
df_with_date = df.withColumn("formatted_date", to_date("date_string", "yyyy-MM-dd"))
df_with_date.show()
```

#### Output Table:

| date_string    | formatted_date |
|----------------|----------------|
| `2025-01-09`  | `2025-01-09`   |
| `01-09-2025`  | `NULL`         |
| `2025/01/09`  | `NULL`         |
| `20250109`    | `NULL`         |

Notice that only the `date_string` matching the `yyyy-MM-dd` format is converted successfully. You can use multiple formats with conditional logic to handle varied inputs.

---

### Processing with `date_format`:

The `date_format` function is used to transform `DateType` or `TimestampType` into a string in a specific pattern.

```python
from pyspark.sql.functions import date_format, to_date

# Convert string to DateType and apply date_format
df_with_date_format = df.withColumn("formatted_date", to_date("date_string", "yyyy-MM-dd"))
df_with_date_format = df_with_date_format.withColumn(
    "formatted_string",
    date_format("formatted_date", "MMM dd, yyyy")
)
df_with_date_format.show()
```

#### Output Table:

| date_string    | formatted_date | formatted_string |
|----------------|----------------|------------------|
| `2025-01-09`  | `2025-01-09`   | `Jan 09, 2025`   |
| `01-09-2025`  | `NULL`         | `NULL`           |
| `2025/01/09`  | `NULL`         | `NULL`           |
| `20250109`    | `NULL`         | `NULL`           |

In this example, `date_format` successfully formats the `formatted_date` column into a readable string, but only if `to_date` could parse the date first.

---

## Key Takeaways

- **`to_date`:** Converts strings or timestamps to a standard `DateType`, enabling further date-based operations (e.g., sorting, filtering).
  - Use it for data normalization and cleaning.
- **`date_format`:** Converts `DateType` or `TimestampType` into a string with a custom format.
  - Use it for creating user-friendly, formatted outputs in reports or visualizations.

# Overview of `date_format`

The `date_format` function allows you to format a `DateType` or `TimestampType` column into a string according to a specified pattern.

## Syntax:
```sql
  date_format(date, format)
```
- `date`: A `DateType` or `TimestampType` column.
- `format`: The desired output format (a string pattern).

## Common Patterns for Formatting

| Pattern  | Description               | Example   |
|----------|---------------------------|-----------|
| `y`      | Year                     | 2025      |
| `M`      | Month in year (1-12)     | 1         |
| `MM`     | Month in year (01-12)    | 01        |
| `MMM`    | Month name short         | Jan       |
| `MMMM`   | Month name full          | January   |
| `d`      | Day of the month (1-31)  | 9         |
| `dd`     | Day of the month (01-31) | 09        |
| `E`      | Day name short           | Mon       |
| `EEEE`   | Day name full            | Monday    |
| `H`      | Hour in day (0-23)       | 14        |
| `h`      | Hour in AM/PM (1-12)     | 2         |
| `a`      | AM/PM marker             | PM        |
| `m`      | Minute in hour (0-59)    | 30        |
| `s`      | Second in minute (0-59)  | 45        |

---

## Example: Using `date_format` in PySpark

### Step 1: Create a DataFrame
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_format

# Initialize Spark session
spark = SparkSession.builder.appName("DateFormatting").getOrCreate()

# Sample data with a date string column
data = [("2025-01-09",), ("2025-12-25",), ("2024-07-07",)]
columns = ["date_string"]
df = spark.createDataFrame(data, columns)

# Step 1: Convert 'date_string' to a DateType column
df = df.withColumn("date", to_date("date_string", "yyyy-MM-dd"))

# Step 2: Apply valid datetime patterns
formatted_df = df.select(
    "date",
    date_format("date", "MMM").alias("Month (Short Name)"),   # Short month name
    date_format("date", "MMMM").alias("Month (Full Name)"),  # Full month name
    date_format("date", "E").alias("Day (Short Name)"),      # Short day name
    date_format("date", "EEEE").alias("Day (Full Name)"),    # Full day name
    date_format("date", "dd/MM/yyyy").alias("Custom Format") # Custom date format
)

# Step 3: Show the results
print("Formatted DataFrame:")
formatted_df.show(truncate=False)

```

#### Output:
```
+-----------+----------+
|date_string|      date|
+-----------+----------+
| 2025-01-09|2025-01-09|
| 1970-01-01|1970-01-01|
| 2000-12-31|2000-12-31|
+-----------+----------+
```

---

### Step 2: Apply `date_format` Function
```python
# Format the date into different string patterns
formatted_df = df.select(
    "date",
    date_format("date", "LLL").alias("Month (Short Name)"),
    date_format("date", "MMMM").alias("Month (Full Name)"),
    date_format("date", "EEE").alias("Day (Short Name)"),
    date_format("date", "EEEE").alias("Day (Full Name)"),
    date_format("date", "dd/MM/yyyy").alias("Custom Format")
)

# Show formatted DataFrame
print("Formatted DataFrame:")
formatted_df.show()
```

#### Output:
```
+----------+-----------------+----------------+----------------+--------------+-------------+
|      date|Month (Short Name)|Month (Full Name)|Day (Short Name)|Day (Full Name)|Custom Format|
+----------+-----------------+----------------+----------------+--------------+-------------+
|2025-01-09|              Jan|         January|             Thu|     Thursday |   09/01/2025|
|1970-01-01|              Jan|         January|             Thu|     Thursday |   01/01/1970|
|2000-12-31|              Dec|         December|             Sun|     Sunday   |   31/12/2000|
+----------+-----------------+----------------+----------------+--------------+-------------+
```

---

## Explanation of the Example

- `date_format("date", "LLL")`:
  - Formats the date to display the short month name (e.g., Jan).

- `date_format("date", "MMMM")`:
  - Displays the full month name (e.g., January).

- `date_format("date", "EEE")`:
  - Displays the short name of the day (e.g., Thu for Thursday).

- `date_format("date", "EEEE")`:
  - Displays the full name of the day (e.g., Thursday).

- `date_format("date", "dd/MM/yyyy")`:
  - Custom format that displays the date in `dd/MM/yyyy` format (e.g., 09/01/2025).

---

## Key Points:

- `date_format` is used to transform dates into string representations for better readability or specific formats.
- It’s often used for generating reports or exporting data where dates need to be in a user-friendly format.
- You can use it in conjunction with other date functions like `to_date` and `current_date`.


