import clickhouse_connect
import subprocess
from time import sleep

client = clickhouse_connect.get_client(host='localhost', username='', password='')

PUBLISHERS = 1000
COUNT = 100
# options ["imu", "bms", "gps"]
DATA_TYPE = "imu"

mqttwrk_run = subprocess.run([
    "./target/debug/mqttwrk", "simulator",
    "-p", str(PUBLISHERS), 
    "--count", str(COUNT),
    "--data-type", DATA_TYPE,
])
total = PUBLISHERS * COUNT

# Clickhouse is eventually consistent so need to wait sometime before quering it
sleep(1)

# query clickhouse if it got that many rows
result = client.command(f"SELECT COUNT(*) from demo.{DATA_TYPE}")

print(result)

if result == total:
    # clear the table
    client.command(f"TRUNCATE demo.{DATA_TYPE}")
else:
    # maybe store a copy of this table in some other table for debugging
    client.command(f"TRUNCATE demo.{DATA_TYPE}")
    raise Exception("something failed")
