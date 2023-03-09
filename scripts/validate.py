import clickhouse_connect
import subprocess
from time import sleep
from slack_sdk import WebClient
from os import environ
from datetime import datetime
import sys

import logging
logging.basicConfig(level=logging.INFO)


PUBLISHERS = 1000
COUNT = 100
# options ["imu", "bms", "gps"]
DATA_TYPE = "imu"


def main():
    now = datetime.now()

    clickhouse_client = clickhouse_connect.get_client(
        host=environ['CLICKHOUSE_HOST'],
        username=environ['CLICKHOUSE_USERNAME'],
        password=environ['CLICKHOUSE_PASSWORD']
    )

    # Clear table before starting test
    clickhouse_client.command(f"TRUNCATE demo.{DATA_TYPE};")

    subprocess.run([
        "/usr/share/bytebeam/mqttwrk/target/release/mqttwrk", "simulator",
        "-p", str(PUBLISHERS), 
        "-s", str(1),
        "--count", str(COUNT),
        "--data-type", DATA_TYPE,
        "-S", "beamd"
    ])

    total = PUBLISHERS * COUNT

    # Wait for data to be flushed to clickhouse
    sleep(15)

    # query clickhouse if it got that many rows
    result = clickhouse_client.command(f"SELECT COUNT(*) from demo.{DATA_TYPE};")

    if result == total:
        logging.info(f"[{now}] Data was flushed to clickhouse properly. Doing nothing.")
        clickhouse_client.command(f"TRUNCATE demo.{DATA_TYPE}")
    else:
        # maybe store a copy of this table in some other table for debugging
        logging.info(f"[{now}] Data was not flushed to clickhouse properly. Trying to send alert on slack.")
        try:
            message_slack(f"<!channel> Expected count to be {total}, but it was {result}")
        except:
            logging.info(f"[{now}] Couldn't send message on slack")
        sys.exit(1)


def message_slack(message):
    slack_client = WebClient(token=environ['SLACK_API_TOKEN'])
    slack_channel = f"#{environ['SLACK_CHANNEL']}"
    slack_client.chat_postMessage(channel=slack_channel, text=message)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.info(f"[{datetime.now()}] Something went wront, Please check. Exception: {e}")

    logging.info(f"[{datetime.now()}] Sleeping for 5 minutes before re-running")
    sleep(5 * 60)
