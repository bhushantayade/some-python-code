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

