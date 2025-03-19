SELECT REGEXP_EXTRACT('file_id checksum value abc xyz', r'checksum\s+(.*)') AS extracted_part;
SELECT SUBSTR('file_id checksum value abc xyz', 
              STRPOS('file_id checksum value abc xyz', 'checksum') + LENGTH('checksum') + 1) 
       AS extracted_part;
SELECT ARRAY_TO_STRING(SPLIT('file_id checksum value abc xyz', 'checksum ')[SAFE_OFFSET(1)], ' ') 
       AS extracted_part;
