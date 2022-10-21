import datetime
from typing import List

ATHENA_DATE_PATTERN = '%Y-%m-%d:%H:%M:%s'

def athena_query(start_time: datetime.date, end_time: datetime.date, s3_bucket: str) -> str:
    return f"""
        WITH counts AS (
        SELECT
            SUM(CAST(REGEXP_LIKE(operation, '^REST.(PUT|COPY|POST|LIST).*$') AS int)) AS num_writes,
            SUM(CAST(REGEXP_LIKE(operation, '^REST.(GET|SELECT).*$') AS int)) AS num_reads,
            SUM(bytessent) AS bytes_written,
            SUM(objectsize) AS bytes_read,
            SUM(bytessent + objectsize) AS total_data
        FROM raysort_logs_db.logs
        WHERE bucket_name LIKE '{s3_bucket}___'
            AND parse_datetime(RequestDateTime,'dd/MMM/yyyy:HH:mm:ss Z')
            BETWEEN parse_datetime('{start_time.strftime(ATHENA_DATE_PATTERN)}','yyyy-MM-dd:HH:mm:ss')
            AND parse_datetime('{end_time.strftime(ATHENA_DATE_PATTERN)}','yyyy-MM-dd:HH:mm:ss')
        ) SELECT
            CAST(counts.num_writes AS double) * 0.005 / 1000 AS write_cost,
            CAST(counts.num_reads AS double) * 0.0004 / 1000 AS read_cost,
            CAST(counts.bytes_read AS double) / CAST(1073741824 AS double) * 0.02 AS data_cost
            FROM counts;
    """