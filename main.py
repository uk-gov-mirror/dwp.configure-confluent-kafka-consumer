import argparse
import boto3
import json
import logging
import os
import requests
import sys
import time

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
        "--storage-class", default="io.confluent.connect.s3.storage.S3Storage"
    )
    parser.add_argument(
        "--format-class", default="io.confluent.connect.s3.format.json.JsonFormat"
    )
    parser.add_argument(
        "--partitioner-class",
        default="io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    )
    parser.add_argument("--topics-dir", default="")
    parser.add_argument("--timestamp-extractor", default="Wallclock")
    parser.add_argument("--partition-duration-ms", default="86400000")
    parser.add_argument("--path-format", default="YYYY'-'MM'-'dd")
    parser.add_argument("--locale", default="GB")
    parser.add_argument("--timezone", default="UTC")
    parser.add_argument(
        "--initial-wait-time",
        default=1,
        help="How long to wait (in seconds) before making the initial REST API call",
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
    if "STORAGE_CLASS" in os.environ:
        _args.storage_class = os.environ["STORAGE_CLASS"]
    if "FORMAT_CLASS" in os.environ:
        _args.format_class = os.environ["FORMAT_CLASS"]
    if "PARTITIONER_CLASS" in os.environ:
        _args.partitioner_class = os.environ["PARTITIONER_CLASS"]
    if "TOPICS_DIR" in os.environ:
        _args.topics_dir = os.environ["TOPICS_DIR"]
    if "TIMESTAMP_EXTRACOTR" in os.environ:
        _args.timestamp_extractor = os.environ["TIMESTAMP_EXTRACTOR"]
    if "PARTITION_DURATION_MS" in os.environ:
        _args.partition_duration_ms = os.environ["PARTITION_DURATION_MS"]
    if "PATH_FORMAT" in os.environ:
        _args.path_format = os.environ["PATH_FORMAT"]
    if "LOCALE" in os.environ:
        _args.locale = os.environ["LOCALE"]
    if "TIMEZONE" in os.environ:
        _args.timezone = os.environ["TIMEZONE"]
    if "INITIAL_WAIT_TIME" in os.environ:
        _args.initial_wait_time = os.environ["INITIAL_WAIT_TIME"]

    _args.initial_wait_time = int(_args.initial_wait_time)
    return _args


def handler(event, context):
    args = get_parameters()
    try:
        configure_confluent_kafka_consumer(event, args)
    except KeyError as key_name:
        logger.error(f"Key: {key_name} is required in payload")
    except Exception as e:
        logger.error("Unexpected error occurred")
        logger.error(e)
        sys.exit(1)


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
        "storage.class": args.storage_class,
        "format.class": args.format_class,
        "partitioner.class": args.partitioner_class,
        "topics.dir": args.topics_dir,
        "timestamp.extractor": args.timestamp_extractor,
        "partition.duration.ms": args.partition_duration_ms,
        "path.format": args.path_format,
        "locale": args.locale,
        "timezone": args.timezone,
    }

    # Confluent's Kafka consumer containers can take a while to start up the
    # REST API, so sleep until it's ready
    logger.info("Waiting for REST API to become available")
    time.sleep(args.initial_wait_time)

    # Get a list of existing connectors
    api_base_url = f"http://{private_ip}:{args.port}/connectors"
    try:
        logger.info("Retrieving list of connectors")
        logger.debug(f"GET request on {api_base_url}")
        response = requests.get(f"{api_base_url}", timeout=1)
    except Exception:
        # If the GET request fails, it's likely the container hasn't finished
        # starting the REST API service, so bail out now as nothing else is
        # going to work
        logger.error(
            f"Error communicating with worker's REST API. Is --initial-wait-time (INITIAL_WAIT_TIME) long enough?"
        )
        raise

    existing_connectors = json.loads(response.text)
    logger.debug(f"Current connectors: {existing_connectors}")

    if args.connector_name in existing_connectors:
        logger.info(f"Getting {args.connector_name} connector config for comparison")
        logger.debug(f"GET request on {api_base_url}/{args.connector_name}/config")
        response = requests.get(f"{api_base_url}/{args.connector_name}/config")
        logger.debug(response.text)
        current_config = json.loads(response.text)
        current_config.pop("name")
        logger.debug(f"current connector config = {current_config}")
        logger.debug(f"requested connector config = {connector_config}")
        if current_config != connector_config:
            logger.info(f"Updating {args.connector_name} connector config")
            logger.debug(f"PUT request on {api_base_url}/{args.connector_name}/config")
            response = requests.put(
                f"{api_base_url}/{args.connector_name}/config", json=connector_config
            )
            logger.debug(response.text)
    else:
        payload = {"name": args.connector_name, "config": connector_config}
        logger.info(f"Creating {args.connector_name} connector")
        logger.debug(f"POST request on {api_base_url}")
        response = requests.post(f"{api_base_url}", json=payload)
        logger.debug(response.text)

    for existing_connector in existing_connectors:
        if existing_connector != args.connector_name:
            logger.info(f"Deleting connector {existing_connector}")
            logger.debug(f"DELETE request on {api_base_url}/{existing_connector}")
            response = requests.delete(f"{api_base_url}/{existing_connector}")
            logger.debug(response.text)


if __name__ == "__main__":
    try:
        json_content = json.loads(open("event.json", "r").read())
    except Exception as e:
        logger.error(e)
        sys.exit(1)
    handler(json_content, None)
