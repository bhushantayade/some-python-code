WITH sample_data AS (
  SELECT 'active/track_order/2023/202311/231115/file.json' AS formatted_path UNION ALL
  SELECT 'archive/202311/reports.csv' UNION ALL
  SELECT 'exports/231115/data.txt' UNION ALL
  SELECT 'static/file.txt' -- No dates to reverse
)

SELECT
  formatted_path,
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        formatted_path,
        r'(\d{4})(\d{2})(\d{2})', -- YYYYMMDD pattern
        'YYYYMMDD'
      ),
      r'(\d{4})(\d{2})', -- YYYYMM pattern
      'YYYYMM'
    ),
    r'\b\d{4}\b', -- YYYY pattern (whole word)
    'YYYY'
  ) AS original_pattern,
  -- Also extract the actual dates if needed
  REGEXP_EXTRACT(formatted_path, r'(\d{4})[/_-](\d{2})[/_-](\d{2})') AS extracted_date,
  REGEXP_EXTRACT(formatted_path, r'(\d{4})(\d{2})') AS extracted_year_month,
  REGEXP_EXTRACT(formatted_path, r'\b(\d{4})\b') AS extracted_year
FROM sample_data


###
SELECT
  original_path,
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        original_path,
        'YYYY', FORMAT_DATE('%Y', CURRENT_DATE())
      ),
      'YYYYMM', FORMAT_DATE('%Y%m', CURRENT_DATE())
    ),
    'YYMMDD', FORMAT_DATE('%y%m%d', CURRENT_DATE())
  ) AS resolved_path
FROM your_table

2.
DECLARE target_date DATE DEFAULT DATE('2023-11-15'); -- Can be parameterized

SELECT
  original_path,
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        original_path,
        'YYYY', FORMAT_DATE('%Y', target_date)
      ),
      'YYYYMM', FORMAT_DATE('%Y%m', target_date)
    ),
    'YYMMDD', FORMAT_DATE('%y%m%d', target_date)
  ) AS resolved_path
FROM your_table

3.
WITH sample_data AS (
  SELECT 'active/track_order/YYYY/YYYYMM/YYMMDD/file.json' AS original_path UNION ALL
  SELECT 'archive/YYYYMM/reports.csv' UNION ALL
  SELECT 'exports/YYMMDD/data.txt' UNION ALL
  SELECT 'static/file.txt' -- No date placeholders
)

SELECT
  original_path,
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        original_path,
        'YYYY', FORMAT_DATE('%Y', CURRENT_DATE())
      ),
      'YYYYMM', FORMAT_DATE('%Y%m', CURRENT_DATE())
    ),
    'YYMMDD', FORMAT_DATE('%y%m%d', CURRENT_DATE())
  ) AS resolved_path
FROM sample_data

####
from datetime import datetime

def resolve_date_path(file_path, target_date=None):
    """
    Enhanced version that properly handles YYYY/YYYYMM/YYYYMMDD formats
    """
    if target_date is None:
        target_date = datetime.now()
    
    # Ordered from most specific to least specific
    format_replacements = [
        ('YYYYMMDD', target_date.strftime('%Y%m%d')),
        ('YYYYMM', target_date.strftime('%Y%m')),
        ('YYYY', target_date.strftime('%Y')),
        ('YY', target_date.strftime('%y')),
        ('MM', target_date.strftime('%m')),
        ('DD', target_date.strftime('%d'))
    ]
    
    for placeholder, replacement in format_replacements:
        file_path = file_path.replace(placeholder, replacement)
    
    return file_path

222222.####

from datetime import datetime
import pytest

# Fixed test date for consistent results (2023-11-15)
TEST_DATE = datetime(2023, 11, 15)

test_cases = [
    # Standard formats
    ("data/YYYY/file.json", "data/2023/file.json"),
    ("logs/YYYYMM/app.log", "logs/202311/app.log"),
    ("exports/YYYYMMDD/data.csv", "exports/20231115/data.csv"),
    ("archive/YYYY/MM/DD/backup.zip", "archive/2023/11/15/backup.zip"),
    
    # Mixed formats
    ("reports/YYYY-MM-DD/daily.pdf", "reports/2023-11-15/daily.pdf"),
    ("temp/YYMMDD/today.dat", "temp/231115/today.dat"),
    ("user/YYYY/MMDD/profile.json", "user/2023/1115/profile.json"),
    
    # Edge cases
    ("YYYYMMDD_filename.txt", "20231115_filename.txt"),
    ("no_date_here.txt", "no_date_here.txt"),
    ("MMDDYY/test.xyz", "111523/test.xyz"),
    ("prefix_YYYY_suffix.dat", "prefix_2023_suffix.dat"),
    
    # Multiple occurrences
    ("YYYY/YYYY/YYYY", "2023/2023/2023"),
    ("YYYYMM/YYYYMM/YYYYMM", "202311/202311/202311"),
    
    # Different delimiters
    ("YYYY.MM.DD_log.txt", "2023.11.15_log.txt"),
    ("YYYY_MM_DD_config.ini", "2023_11_15_config.ini"),
    
    # Partial matches (shouldn't replace)
    ("MY_YYYY_DATA", "MY_2023_DATA"),
    ("PLACEHOLDER_MM", "PLACEHOLDER_11"),
    
    # Case sensitivity
    ("yyyy/mm/dd/file.txt", "yyyy/11/15/file.txt"),  # Only upper case handled
    ("yyyymmdd_file.txt", "yyyymmdd_file.txt"),      # Not matched
]

@pytest.mark.parametrize("input_path,expected", test_cases)
def test_resolve_date_path(input_path, expected):
    assert resolve_date_path(input_path, TEST_DATE) == expected

# Special format test
def test_special_format_combinations():
    assert resolve_date_path("A/YYYY/B/YYYYMM/C/YYYYMMDD/D", TEST_DATE) == "A/2023/B/202311/C/20231115/D"
    assert resolve_date_path("X_YY_MM_DD_Y", TEST_DATE) == "X_23_11_15_Y"
    assert resolve_date_path("1YYYY2MM3DD4", TEST_DATE) == "12023112154"

# Real-world example test
def test_real_world_path():
    path = "active/track_order/YYYY/YYYYMM/YYYYMMDD/file.json"
    expected = "active/track_order/2023/202311/20231115/file.json"
    assert resolve_date_path(path, TEST_DATE) == expected

# Current date test
def test_current_date_default():
    today = datetime.now()
    path = "logs/YYYY/MM/DD.log"
    expected = f"logs/{today.year}/{today.month:02d}/{today.day:02d}.log"
    assert resolve_date_path(path) == expected

# Run the tests (if executed directly)
if __name__ == "__main__":
    import sys
    pytest.main(sys.argv)





######

SELECT *
FROM `your_project.your_dataset.your_table`
WHERE timestamp_column >= TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR), INTERVAL 1 HOUR)
  AND timestamp_column < TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR)



import csv
import pandas as pd
from google.cloud import storage

def infer_data_type(value):
    """Infer data type of a value."""
    try:
        int(value)
        return "int"
    except ValueError:
        try:
            float(value)
            return "float"
        except ValueError:
            try:
                pd.to_datetime(value)
                return "date"
            except ValueError:
                return "str"

def generate_schema_from_gcs_csv(bucket_name, file_name):
    """Read CSV from GCS and generate a schema based on the first row."""
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    # Download the CSV file as a string and read it using csv.DictReader
    csv_data = blob.download_as_text()
    reader = csv.DictReader(csv_data.splitlines())
    
    schema = {}
    first_row = next(reader)  # Read the first row of data
    
    for column, value in first_row.items():
        schema[column] = infer_data_type(value)
    
    return schema

def validate_schema(generated_schema, existing_schema):
    """Compare the generated schema with the existing schema."""
    for column, data_type in existing_schema.items():
        if column not in generated_schema:
            print(f"Missing column: {column}")
        elif generated_schema[column] != data_type:
            print(f"Mismatch in column '{column}': Expected {data_type}, got {generated_schema[column]}")
        else:
            print(f"Column '{column}' matches the expected schema.")
    
    for column in generated_schema:
        if column not in existing_schema:
            print(f"Unexpected column in CSV: {column}")

# Example: Existing schema to validate against
existing_schema = {
    "name": "str",
    "age": "int",
    "salary": "float",
    "date_of_joining": "date"
}

# Google Cloud Storage details
bucket_name = "your-bucket-name"
file_name = "path/to/yourfile.csv"

# Generate schema from GCS CSV file
generated_schema = generate_schema_from_gcs_csv(bucket_name, file_name)

# Validate generated schema with existing schema
validate_schema(generated_schema, existing_schema)


##### 10-09-2024 tsv code
import csv
from google.cloud import storage
from datetime import datetime

def infer_type(value):
    """
    Function to infer the data type based on the value.
    """
    try:
        # Check for INTEGER
        int(value)
        return "INTEGER"
    except ValueError:
        try:
            # Check for FLOAT
            float(value)
            return "FLOAT"
        except ValueError:
            # Check for BOOLEAN
            if value.lower() in ["true", "false"]:
                return "BOOLEAN"
            # Check for DATE (try common date formats)
            try:
                datetime.strptime(value, "%Y-%m-%d")  # Change format if needed
                return "DATE"
            except ValueError:
                # If none of the above, treat as STRING
                return "STRING"

def infer_tsv_schema(file_content):
    """
    Function to infer the schema of a TSV file content.
    """
    reader = csv.reader(file_content.splitlines(), delimiter='\t')

    # Read the first row (assumed to be column names)
    try:
        header = next(reader)
    except StopIteration:
        raise ValueError("TSV file is empty or not properly formatted")

    # Read the second row to infer data types
    try:
        first_row = next(reader)
    except StopIteration:
        raise ValueError("TSV file doesn't contain any data rows")

    inferred_schema = {}
    for col, value in zip(header, first_row):
        inferred_schema[col] = infer_type(value)

    return inferred_schema

def download_tsv_from_gcs(bucket_name, file_name):
    """
    Download the TSV file from GCS and return its content as a string.
    """
    client = storage.Client()
    try:
        bucket = client.get_bucket(bucket_name)
    except Exception as e:
        raise ValueError(f"Failed to access bucket: {bucket_name}. Error: {str(e)}")
    
    blob = bucket.get_blob(file_name)
    if not blob:
        raise ValueError(f"File {file_name} not found in bucket {bucket_name}")

    # Download the file content as a string
    try:
        file_content = blob.download_as_text()
        return file_content
    except Exception as e:
        raise ValueError(f"Failed to download file {file_name}. Error: {str(e)}")

# Example usage
bucket_name = "your-gcs-bucket-name"
file_name = "path/to/your-file.tsv"

try:
    # Download the TSV file from GCS
    file_content = download_tsv_from_gcs(bucket_name, file_name)

    # Infer the TSV schema
    inferred_schema = infer_tsv_schema(file_content)

    # Print the inferred schema
    print("Inferred Schema:")
    for col_name, col_type in inferred_schema.items():
        print(f"Column: {col_name}, Type: {col_type}")
except ValueError as e:
    print(f"Error: {str(e)}")

