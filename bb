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
