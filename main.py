import argparse
import boto3
import json
import logging
import os
import requests
import sys
import time

from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# Initialise logging
logger = logging.getLogger(__name__)
log_level = os.environ["LOG_LEVEL"] if "LOG_LEVEL" in os.environ else "ERROR"
logger.setLevel(logging.getLevelName(log_level.upper()))
logging.basicConfig(
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(module)s "
    "%(process)s[%(thread)s] %(message)s",
)
logger.info("Logging at {} level".format(log_level.upper()))


def get_parameters():
    parser = argparse.ArgumentParser(
        description="Configure Confluent Kafka Consumers using RESTful API"
    )

    # Parse command line inputs and set defaults
    parser.add_argument("--connector-name", default="s3-sink")
    parser.add_argument("--aws-profile", default="default")
    parser.add_argument("--aws-region", default="eu-west-2")
    parser.add_argument("--tasks-max", default="1")
    parser.add_argument("--topics", default="")
    parser.add_argument("--flush-size", default="1")
    parser.add_argument("--port", default="8083")
    parser.add_argument("--s3-bucket-name", default="")
    parser.add_argument(
        "--initial-wait-time",
        default=1,
        help="How long to wait (in seconds) before making the initial REST API call",
    )
    parser.add_argument("--retry-attempts", default=3)
    parser.add_argument(
        "--retry-backoff-factor",
        default=0,
        help="Backoff rate for retries (see https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#module-urllib3.util.retry)",
    )

    _args = parser.parse_args()

    # Override arguments with environment variables where set
    if "CONNECTOR_NAME" in os.environ:
        _args.connector_name = os.environ["CONNECTOR_NAME"]
    if "AWS_PROFILE" in os.environ:
        _args.aws_profile = os.environ["AWS_PROFILE"]
    if "AWS_REGION" in os.environ:
        _args.aws_region = os.environ["AWS_REGION"]
    if "TASKS_MAX" in os.environ:
        _args.tasks_max = os.environ["TASKS_MAX"]
    if "TOPICS" in os.environ:
        _args.topics = os.environ["TOPICS"]
    if "FLUSH_SIZE" in os.environ:
        _args.flush_size = os.environ["FLUSH_SIZE"]
    if "PORT" in os.environ:
        _args.port = os.environ["PORT"]
    if "S3_BUCKET_NAME" in os.environ:
        _args.s3_bucket_name = os.environ["S3_BUCKET_NAME"]
    if "INITIAL_WAIT_TIME" in os.environ:
        _args.initial_wait_time = os.environ["INITIAL_WAIT_TIME"]
    if "RETRY_ATTEMPTS" in os.environ:
        _args.retry_attempts = os.environ["RETRY_ATTEMPTS"]
    if "RETRY_BACKOFF_FACTOR" in os.environ:
        _args.retry_backoff_factor = os.environ["RETRY_BACKOFF_FACTOR"]
    return _args


def handler(event, context):
    args = get_parameters()
    try:
        configure_confluent_kafka_consumer(event, args)
    except KeyError as key_name:
        logger.error(f"Key: {key_name} is required in payload")


def configure_confluent_kafka_consumer(event, args):
    if "AWS_PROFILE" in os.environ:
        boto3.setup_default_session(
            profile_name=args.aws_profile, region_name=args.aws_region
        )

    if logger.isEnabledFor(logging.DEBUG):
        # Log everything from boto3
        boto3.set_stream_logger()
    logger.debug(f"Using boto3 {boto3.__version__}")

    logger.debug(event)

    message = json.loads(event["Records"][0]["Sns"]["Message"])
    logger.debug(message)

    private_ip = message["detail"]["containers"][0]["networkInterfaces"][0][
        "privateIpv4Address"
    ]
    logger.debug(private_ip)

    connector_config = {
        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
        "tasks.max": args.tasks_max,
        "topics": args.topics,
        "flush.size": args.flush_size,
        "s3.region": args.aws_region,
        "s3.bucket.name": args.s3_bucket_name,
        "storage.class": "io.confluent.connect.s3.storage.S3Storage",
        "format.class": "io.confluent.connect.s3.format.avro.AvroFormat",
        "schema.generator.class": "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
        "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
    }

    # Confluent's Kafka consumer containers can take a while to start up the
    # REST API, so configure requests to retry the initial API call
    s = requests.Session()
    retries = Retry(total=args.retry_attempts, backoff_factor=args.retry_backoff_factor)
    s.mount("http://", HTTPAdapter(max_retries=retries))

    # Get a list of existing connectors
    response = s.get(f"http://{private_ip}:{args.port}/connectors")

    existing_connectors = json.loads(response.text)

    # Check if required connector already exists

    if args.connector_name in existing_connectors:
        logger.debug("update connector [PUT]")
        # PUT payload to update existing connector
        response = requests.put(
            f"http://{private_ip}:{args.port}/connectors/{args.connector_name}/config",
            json=connector_config,
        )
        logger.debug(response.text)

    else:

        payload = {"name": args.connector_name, "config": connector_config}

        # POST payload if connectors don't exist

        logger.debug("create connector [POST]")

        response = requests.post(
            f"http://{private_ip}:{args.port}/connectors", json=payload
        )
        logger.debug(response.text)

    # DELETE all others

    for existing_connector in existing_connectors:
        if existing_connector != args.connector_name:

            logger.debug("delete connector [DELETE]")

            response = requests.delete(
                f"http://{private_ip}:{args.port}/connectors/{existing_connector}"
            )
            logger.debug(response.text)


if __name__ == "__main__":
    try:
        json_content = json.loads(open("event.json", "r").read())
        handler(json_content, None)
    except Exception as e:
        logger.error("Unexpected error occurred")
        logger.error(e)
        raise e
