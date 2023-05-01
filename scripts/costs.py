from datetime import datetime
import os
import sys
from typing import Any, Tuple
import time

import boto3
import wandb


ATHENA_DATE_PATTERN = "%Y-%m-%d:%H:%M:%s"
CLIENT = boto3.client("athena")
LOG_BUCKET = os.getenv("LOG_BUCKET")
S3_BUCKET = os.getenv("S3_BUCKET")


def get_client() -> Any:
    return CLIENT


def table_name(bucket_name: str) -> str:
    return bucket_name.replace("-", "_")


def run_query(query_string) -> str:
    """Returns the ID of an attempted query."""
    client = get_client()
    id = client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={"Database": table_name(LOG_BUCKET)},
        ResultConfiguration={"OutputLocation": f"s3://{LOG_BUCKET}/athena-output"},
    )
    return id


def create_table() -> None:
    """Attempts to create a table in Athena for S3 access logs if needed."""
    db = table_name(LOG_BUCKET)
    tbl = table_name(S3_BUCKET)
    query_string = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS `{db}.{tbl}` (
        `bucketowner` STRING,
        `bucket_name` STRING,
        `requestdatetime` STRING,
        `remoteip` STRING,
        `requester` STRING,
        `requestid` STRING,
        `operation` STRING,
        `key` STRING,
        `request_uri` STRING,
        `httpstatus` STRING,
        `errorcode` STRING,
        `bytessent` BIGINT,
        `objectsize` BIGINT,
        `totaltime` STRING,
        `turnaroundtime` STRING,
        `referrer` STRING,
        `useragent` STRING,
        `versionid` STRING,
        `hostid` STRING,
        `sigv` STRING,
        `ciphersuite` STRING,
        `authtype` STRING,
        `endpoint` STRING,
        `tlsversion` STRING)
        ROW FORMAT SERDE
        'org.apache.hadoop.hive.serde2.RegexSerDe'
        WITH SERDEPROPERTIES (
        'input.regex'='([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$')
        STORED AS INPUTFORMAT
        'org.apache.hadoop.mapred.TextInputFormat'
        OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION
        's3://{LOG_BUCKET}/{S3_BUCKET}/'"""
    run_query(query_string)


def get_run_timestamps(wandb_run: str) -> Tuple[datetime, datetime]:
    """Gets the start and end time of a sort from a W&B run id."""
    api = wandb.Api()
    try:
        run = api.run(f"raysort/raysort/runs/{wandb_run}")
        start_time = run.summary["_timestamp"]
        end_time = start_time + run.summary["sort"]
    except Exception as e:
        print(e)
        return ""
    start = datetime.fromtimestamp(start_time)
    end = datetime.fromtimestamp(end_time)
    return start, end


def get_query_costs(wandb_run: str) -> None:
    """Queries Athena for the costs of S3 access logs and prints the location of the output.

    May not be accurate until several hours after the run.
    """
    create_table()
    db = table_name(LOG_BUCKET)
    tbl = table_name(S3_BUCKET)
    start, end = get_run_timestamps(wandb_run)
    query_string = f"""
        WITH counts AS (
        SELECT
            SUM(CAST(REGEXP_LIKE(operation, '^REST.(PUT|COPY|POST|LIST).*$') AS int)) AS num_writes,
            SUM(CAST(REGEXP_LIKE(operation, '^REST.(GET|SELECT).*$') AS int)) AS num_reads,
            SUM(bytessent) AS bytes_written,
            SUM(objectsize) AS bytes_read,
            SUM(bytessent + objectsize) AS total_data
        FROM {db}.{tbl}
        WHERE parse_datetime(RequestDateTime,'dd/MMM/yyyy:HH:mm:ss Z')
            BETWEEN parse_datetime('{start.strftime(ATHENA_DATE_PATTERN)[:19]}','yyyy-MM-dd:HH:mm:ss')
            AND parse_datetime('{end.strftime(ATHENA_DATE_PATTERN)[:19]}','yyyy-MM-dd:HH:mm:ss')
        ) SELECT
            CAST(counts.num_writes AS double) * 0.005 / 1000 AS write_cost,
            CAST(counts.num_reads AS double) * 0.0004 / 1000 AS read_cost,
            CAST(counts.bytes_read AS double) / CAST(1073741824 AS double) * 0.02 AS data_cost
            FROM counts;
    """
    query = run_query(query_string)
    time.sleep(5)
    execution = get_client().get_query_execution(
        QueryExecutionId=query["QueryExecutionId"]
    )
    print(
        f"S3 costs are available at {execution['ResultConfiguration']['OutputLocation']}"
    )


if __name__ == "__main__":
    assert len(sys.argv) == 2, "first and only argument must be W&B run id"
    get_query_costs(sys.argv[1])
