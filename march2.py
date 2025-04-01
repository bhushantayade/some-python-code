#
resource.type="gcs_bucket"
protoPayload.methodName="storage.objects.create"
protoPayload.status.message:"Unavailable"
severity=ERROR
###
CREATE OR REPLACE VIEW `your_project.error_logs.recent_gcs_upload_failures` AS
SELECT
  timestamp,
  protoPayload.methodName AS operation,
  protoPayload.resourceName AS resource,
  protoPayload.status.message AS error_message,
  severity,
  JSON_VALUE(protoPayload.metadata, '$.bucket') AS bucket_name,
  JSON_VALUE(protoPayload.metadata, '$.object') AS object_name
FROM
  `your_project.your_log_dataset.cloudaudit_googleapis_com_data_access_*`
WHERE
  protoPayload.methodName LIKE 'storage.objects.%'
  AND severity = 'ERROR'
  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY
  timestamp DESC;

22###
CREATE OR REPLACE VIEW `your_project.error_logs.realtime_gcs_failures` AS
WITH latest_logs AS (
  SELECT *
  FROM `your_project.error_logs.gcs_upload_errors`
  WHERE TIMESTAMP(timestamp) >= TIMESTAMP_SUB(
    CURRENT_TIMESTAMP(), 
    INTERVAL 15 MINUTE
  )
)
SELECT
  TIMESTAMP(timestamp) AS error_time,
  operation,
  bucket_name,
  file_name,
  error_type,
  error_message,
  attempt_number,
  custom_metadata,
  CASE
    WHEN error_type = 'ServiceUnavailable' THEN 'RETRYABLE'
    ELSE 'CRITICAL'
  END AS error_category
FROM latest_logs
ORDER BY error_time DESC;



####error log code######
import datetime
import time
import traceback
from google.api_core import exceptions, retry
from google.cloud import storage, bigquery
from typing import Optional, Dict, Any

class GCSUploaderWithErrorLogging:
    """Handles GCS uploads with retries and BigQuery error logging."""
    
    def __init__(
        self,
        bq_dataset: str = "error_logs",
        bq_table: str = "gcs_upload_errors",
        max_retries: int = 3,
        initial_retry_delay: float = 1.0,
        retry_multiplier: float = 2.0
    ):
        """
        Args:
            bq_dataset: BigQuery dataset name for error logs
            bq_table: BigQuery table name for error logs
            max_retries: Maximum number of retry attempts
            initial_retry_delay: Initial delay between retries in seconds
            retry_multiplier: Multiplier for exponential backoff
        """
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        self.bq_table_ref = f"{self.bq_client.project}.{bq_dataset}.{bq_table}"
        self.max_retries = max_retries
        self.initial_retry_delay = initial_retry_delay
        self.retry_multiplier = retry_multiplier
        
        # Ensure the error log table exists
        self._create_error_log_table_if_not_exists()

    def _create_error_log_table_if_not_exists(self):
        """Create the error log table if it doesn't exist."""
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("operation", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("bucket_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("file_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("error_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("error_message", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("attempt_number", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("stack_trace", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("custom_metadata", "JSON", mode="NULLABLE")
        ]
        
        table = bigquery.Table(self.bq_table_ref, schema=schema)
        try:
            self.bq_client.create_table(table, exists_ok=True)
        except exceptions.GoogleAPICallError as e:
            print(f"Warning: Could not verify/create error log table: {e}")

    def _log_error_to_bigquery(
        self,
        operation: str,
        error: Exception,
        attempt_number: int,
        bucket_name: Optional[str] = None,
        file_name: Optional[str] = None,
        custom_metadata: Optional[Dict[str, Any]] = None
    ):
        """Logs error details to BigQuery."""
        error_data = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "operation": operation,
            "bucket_name": bucket_name,
            "file_name": file_name,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "attempt_number": attempt_number,
            "stack_trace": traceback.format_exc(),
            "custom_metadata": custom_metadata
        }
        
        try:
            errors = self.bq_client.insert_rows_json(
                self.bq_table_ref,
                [error_data],
                retry=retry.Retry(deadline=30)  # Retry for BigQuery insert
            )
            if errors:
                print(f"Failed to insert error log: {errors}")
        except Exception as e:
            print(f"Critical: Failed to log error to BigQuery: {e}")

    @retry.Retry(
        predicate=retry.if_exception_type(
            exceptions.ServiceUnavailable,
            exceptions.TooManyRequests,
            exceptions.GatewayTimeout
        )
    )
    def upload_file(
        self,
        bucket_name: str,
        source_file_path: str,
        destination_blob_name: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        custom_metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Uploads a file to GCS with retry logic and error logging.
        
        Args:
            bucket_name: Name of the GCS bucket
            source_file_path: Local path to the file
            destination_blob_name: Destination path in GCS (defaults to source filename)
            metadata: Optional metadata for the blob
            custom_metadata: Additional metadata for error logging
            
        Returns:
            bool: True if upload succeeded, False if failed after retries
        """
        destination_blob_name = destination_blob_name or source_file_path.split("/")[-1]
        
        for attempt in range(1, self.max_retries + 1):
            try:
                bucket = self.storage_client.bucket(bucket_name)
                blob = bucket.blob(destination_blob_name)
                
                if metadata:
                    blob.metadata = metadata
                
                blob.upload_from_filename(source_file_path)
                print(f"Successfully uploaded {source_file_path} to gs://{bucket_name}/{destination_blob_name}")
                return True
                
            except (exceptions.ServiceUnavailable,
                   exceptions.TooManyRequests,
                   exceptions.GatewayTimeout) as e:
                
                if attempt == self.max_retries:
                    self._log_error_to_bigquery(
                        operation="gcs_upload",
                        error=e,
                        attempt_number=attempt,
                        bucket_name=bucket_name,
                        file_name=source_file_path,
                        custom_metadata=custom_metadata
                    )
                    print(f"Failed to upload after {attempt} attempts: {e}")
                    return False
                
                self._log_error_to_bigquery(
                    operation="gcs_upload_retry",
                    error=e,
                    attempt_number=attempt,
                    bucket_name=bucket_name,
                    file_name=source_file_path,
                    custom_metadata=custom_metadata
                )
                
                delay = self.initial_retry_delay * (self.retry_multiplier ** (attempt - 1))
                print(f"Attempt {attempt} failed. Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
                
            except Exception as e:
                self._log_error_to_bigquery(
                    operation="gcs_upload",
                    error=e,
                    attempt_number=attempt,
                    bucket_name=bucket_name,
                    file_name=source_file_path,
                    custom_metadata=custom_metadata
                )
                print(f"Non-retriable error during upload: {e}")
                return False
        
        return False

    def close(self):
        """Clean up clients."""
        self.storage_client.close()
        self.bq_client.close()

# Example usage
if __name__ == "__main__":
    uploader = GCSUploaderWithErrorLogging(
        bq_dataset="error_logs",
        bq_table="gcs_upload_errors",
        max_retries=5,
        initial_retry_delay=1.0,
        retry_multiplier=2.0
    )
    
    try:
        success = uploader.upload_file(
            bucket_name="my-bucket",
            source_file_path="/path/to/local/file.txt",
            destination_blob_name="destination/path/file.txt",
            metadata={"content_type": "text/plain"},
            custom_metadata={
                "environment": "production",
                "upload_source": "data-pipeline-1"
            }
        )
        
        if not success:
            print("Upload failed after retries")
    finally:
        uploader.close()




####### upload script#####
import os
import threading
import time
from google.cloud import storage
from queue import Queue
from collections import defaultdict

class GCSMultiUploader:
    def __init__(self, bucket_name, max_workers=5, max_retries=3):
        """
        Initialize the GCS multi-file uploader with threading.
        
        Args:
            bucket_name (str): Name of the GCS bucket
            max_workers (int): Number of worker threads
            max_retries (int): Maximum number of retry attempts per file
        """
        self.bucket_name = bucket_name
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.queue = Queue()
        self.lock = threading.Lock()
        self.upload_stats = {
            'success': 0,
            'failed': 0,
            'retries': 0,
            'attempts': defaultdict(int)
        }
        self.workers = []

    def upload_file(self, file_path, gcs_path=None, retry_count=0):
        """
        Upload a single file to GCS with retry logic.
        
        Args:
            file_path (str): Local path to the file
            gcs_path (str): Destination path in GCS (None for same as local)
            retry_count (int): Current retry attempt number
        """
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")

            # Determine GCS destination path
            destination_blob_name = gcs_path if gcs_path else os.path.basename(file_path)
            
            # Upload the file
            blob = self.bucket.blob(destination_blob_name)
            blob.upload_from_filename(file_path)
            
            with self.lock:
                self.upload_stats['success'] += 1
                self.upload_stats['attempts'][file_path] = retry_count + 1
                print(f"Successfully uploaded {file_path} (attempt {retry_count + 1})")
                
        except Exception as e:
            with self.lock:
                self.upload_stats['attempts'][file_path] = retry_count + 1
                self.upload_stats['retries'] += 1
                print(f"Error uploading {file_path} (attempt {retry_count + 1}): {str(e)}")
            
            if retry_count < self.max_retries - 1:
                # Requeue for retry
                self.queue.put((file_path, gcs_path, retry_count + 1))
            else:
                with self.lock:
                    self.upload_stats['failed'] += 1
                print(f"Max retries reached for {file_path}. Giving up.")

    def worker(self):
        """Worker thread function to process upload tasks from the queue."""
        while True:
            task = self.queue.get()
            if task is None:  # Sentinel value to exit
                self.queue.task_done()
                break
                
            file_path, gcs_path, retry_count = task
            self.upload_file(file_path, gcs_path, retry_count)
            self.queue.task_done()

    def add_file(self, file_path, gcs_path=None):
        """Add a file to the upload queue."""
        self.queue.put((file_path, gcs_path, 0))

    def start_workers(self):
        """Start the worker threads."""
        self.workers = []
        for _ in range(self.max_workers):
            worker = threading.Thread(target=self.worker)
            worker.start()
            self.workers.append(worker)

    def wait_completion(self):
        """Wait for all uploads to complete and stop workers."""
        # Wait for all tasks to be processed
        self.queue.join()
        
        # Stop workers
        for _ in range(self.max_workers):
            self.queue.put(None)
        for worker in self.workers:
            worker.join()
            
        return self.upload_stats

def upload_files_to_gcs(file_paths, bucket_name, max_workers=5, max_retries=3):
    """
    Upload multiple files to GCS using multi-threading with retry capability.
    
    Args:
        file_paths (list): List of file paths to upload (or list of (local_path, gcs_path) tuples)
        bucket_name (str): GCS bucket name
        max_workers (int): Number of concurrent upload threads
        max_retries (int): Maximum retry attempts per file
        
    Returns:
        dict: Upload statistics
    """
    uploader = GCSMultiUploader(bucket_name, max_workers, max_retries)
    uploader.start_workers()
    
    # Add files to queue
    for item in file_paths:
        if isinstance(item, tuple):
            local_path, gcs_path = item
            uploader.add_file(local_path, gcs_path)
        else:
            uploader.add_file(item)
    
    # Wait for completion and get stats
    stats = uploader.wait_completion()
    
    # Print summary
    print("\nUpload Summary:")
    print(f"Successful uploads: {stats['success']}")
    print(f"Failed uploads: {stats['failed']}")
    print(f"Total retry attempts: {stats['retries']}")
    print("\nFile attempt counts:")
    for file_path, attempts in stats['attempts'].items():
        print(f"{file_path}: {attempts} attempt(s)")
    
    return stats

if __name__ == "__main__":
    # Example usage
    bucket_name = "your-bucket-name"
    
    # List of files to upload - can be just paths or (local_path, gcs_path) tuples
    files_to_upload = [
        "/path/to/local/file1.txt",
        "/path/to/local/file2.jpg",
        ("/path/to/local/file3.pdf", "custom/path/file3.pdf"),
        # Add more files as needed
    ]
    
    # Upload files with 5 workers and max 3 retries per file
    upload_stats = upload_files_to_gcs(
        file_paths=files_to_upload,
        bucket_name=bucket_name,
        max_workers=5,
        max_retries=3
    )


############ old code##########
SELECT 
    id, 
    COALESCE(first, '') AS first, 
    COALESCE(second, '') AS second
FROM (
    SELECT id, ARRAY_AGG(first IGNORE NULLS)[SAFE_OFFSET(0)] AS first, 
                ARRAY_AGG(second IGNORE NULLS)[SAFE_OFFSET(0)] AS second
    FROM your_table
    GROUP BY id
);

SELECT 
    id, 
    ARRAY_AGG(NULLIF(first, '') IGNORE NULLS)[SAFE_OFFSET(0)] AS first,
    ARRAY_AGG(NULLIF(second, '') IGNORE NULLS)[SAFE_OFFSET(0)] AS second
FROM your_table
GROUP BY id;
