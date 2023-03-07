import clickhouse_connect
import subprocess
from time import sleep
from slack_sdk import WebClient
from os import environ
from datetime import datetime

import logging
logging.basicConfig(level=logging.INFO)


PUBLISHERS = 1000
COUNT = 100
# options ["imu", "bms", "gps"]
DATA_TYPE = "imu"


def main():
    now = datetime.now()

    clickhouse_client = clickhouse_connect.get_client(host='localhost', username='', password='')

    mqttwrk_run = subprocess.run([
        "./target/debug/mqttwrk", "simulator",
        "-p", str(PUBLISHERS), 
        "--count", str(COUNT),
        "--data-type", DATA_TYPE,
    ])
    total = PUBLISHERS * COUNT

    # Wait for data to be flushed to clickhouse
    sleep(1)

    # query clickhouse if it got that many rows
    result = clickhouse_client.command(f"SELECT COUNT(*) from demo.{DATA_TYPE}")

    print(result)

    if result == total:
        logging.info(f"[{now}] Data was flushed to clickhouse properly. Doing nothing.")
        # clear the table
        clickhouse_client.command(f"TRUNCATE demo.{DATA_TYPE}")
    else:
        # maybe store a copy of this table in some other table for debugging
        message_slack(f"Expected count to be {total}, but it was {result}")
        clickhouse_client.command(f"TRUNCATE demo.{DATA_TYPE}")
        logging.info(f"[{now}] Data was not flushed to clickhouse properly. Message sent on slack.")
        raise Exception("something failed")


def message_slack(message):
    slack_client = WebClient(token=environ['SLACK_API_TOKEN'])
    slack_channel = f"#{environ['SLACK_CHANNEL']}"
    slack_client.chat_postMessage(channel=slack_channel, text=message)

if __name__ == "__main__":
    main()
