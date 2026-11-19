"""
AWS Lambda Trigger Handler for Automated E-Commerce Analytics Pipeline.
Invoked by Amazon S3 ObjectCreated events on the 'bronze/' prefix.
Initiates the AWS Glue PySpark ETL Job with required S3 arguments.
"""

import json
import os
import urllib.parse
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client("glue")

GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "ecommerce-medallion-etl")
ALLOWED_PREFIX = "bronze/"
ALLOWED_EXTENSIONS = (".csv", ".json")

def lambda_handler(event, context):
    logger.info("Received S3 Event: %s", json.dumps(event))
    
    try:
        # Extract record metadata
        records = event.get("Records", [])
        if not records:
            logger.warning("No records found in event payload.")
            return {"statusCode": 200, "body": "No records to process."}

        for record in records:
            bucket_name = record["s3"]["bucket"]["name"]
            raw_key = record["s3"]["object"]["key"]
            object_key = urllib.parse.unquote_plus(raw_key)

            logger.info("New object arrived: s3://%s/%s", bucket_name, object_key)

            # Anti-recursion safety filter: strictly process bronze/ files
            if not object_key.startswith(ALLOWED_PREFIX):
                logger.info("Ignoring object outside of %s prefix: %s", ALLOWED_PREFIX, object_key)
                continue

            if not object_key.lower().endswith(ALLOWED_EXTENSIONS):
                logger.info("Ignoring non-data file: %s", object_key)
                continue

            # Start Glue PySpark ETL Job
            logger.info("Starting Glue Job: %s", GLUE_JOB_NAME)
            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={
                    "--S3_BUCKET": bucket_name,
                    "--TRIGGER_KEY": object_key,
                    "--JOB_RUN_MODE": "EVENT_TRIGGERED"
                }
            )

            job_run_id = response.get("JobRunId")
            logger.info("Glue Job successfully initiated. JobRunId: %s", job_run_id)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Glue job triggered successfully", "job_run_id": job_run_id})
        }

    except Exception as e:
        logger.error("Error triggering Glue Job: %s", str(e), exc_info=True)
        raise e
