from hbmqtt.client import MQTTClient, ConnectException, ClientException
from hbmqtt.mqtt.constants import QOS_0
import os
import json
import asyncio
import sys
import argparse
import collections
from mqtt_ble_deduplicator import dedup
import time

parser = argparse.ArgumentParser(description='Read one MQTT topic, deduplicate messages (compare source address and data content), post deduplicated messages to another topic.')
parser.add_argument('--quiet', dest='quiet', action='store_const',
                    const=True, default=False,
                    help="Quiet mode, don't print messages to stdout")

mqtt_source_topic = os.environ.get("MQTT_SOURCE_TOPIC", "/home/ble")
mqtt_target_topic = os.environ.get("MQTT_TARGET_TOPIC", "/home/ble-deduped")

args = parser.parse_args()
client = MQTTClient()

last_activity_timestamp = time.time()
dedup_buffers = collections.defaultdict(lambda: collections.defaultdict(lambda: dedup.Deduplicator()))

def debug(*args):
    if os.environ.get("BLE_DEBUG", None) != None:
        print(*args, file=sys.stdout)

async def main():
    mqtt_url = os.environ.get("MQTT_URL", "mqtt://localhost/")
    print("Connecting to", mqtt_url)
    try:
        await client.connect(mqtt_url)
        print("Connected")
    except Exception as e:
        print("Connection failed: %s" % e)
        asyncio.get_event_loop().stop()
    await client.subscribe([
        (mqtt_source_topic, QOS_0),
        ])
    print("Subscribed")
    counter = 0
    duplicates = 0
    normals = 0
    while True:
        try:
            message = await client.deliver_message()
            data = message.data

            ble_msg = json.loads(data.decode('utf-8'))

            message_receiver = ble_msg['receiver_mac']
            mac = ble_msg['address']['address']
            content = ble_msg['service_data'] or ble_msg['mfg_data']
        except Exception as e:
            print("Data error", e)
            continue

        try:
            mac_buffers = dedup_buffers[mac]
            duplicate = False
            duplicate_receiver = None

            mac_buffers[message_receiver].add(content)
            for buffer_receiver, buffer in mac_buffers.items():
                if buffer_receiver != message_receiver:
                    duplicate_receiver = buffer_receiver
                    duplicate |= buffer.check(content)
        except Exception as e:
            print("Dedupe error", e)
            continue
        
        try:
            if duplicate:
                duplicates += 1
                debug("Got duplicate from %s, previous receivers: %s, mac: %s" %(message_receiver, duplicate_receiver, mac), content.encode('iso-8859-1').hex())
                pass
            else:
                normals += 1
                debug("Got first instance from %s, mac: %s" %( message_receiver, mac), content.encode('iso-8859-1').hex())
                global last_activity_timestamp
                last_activity_timestamp = time.time()
                await client.publish(mqtt_target_topic, data)

            counter += 1

            if counter % 500 == 0:
                print("Processed %d, duplicates: %d" %(counter, duplicates))
        except Exception as e:
            print("MQTT error", e)

ACTIVITY_TIMEOUT = 30
async def watchdog():
    print("Starting watchdog")
    while True:
        now = time.time()
        since_last_activity = (now - last_activity_timestamp)

        if since_last_activity > ACTIVITY_TIMEOUT:
            print("No non-duplicate messages in %d seconds, exiting" %ACTIVITY_TIMEOUT)
            sys.exit(1)

        await asyncio.sleep(1)

def init():
    asyncio.ensure_future(watchdog())
    asyncio.ensure_future(main())
    asyncio.get_event_loop().run_forever()

if __name__ == '__main__':
    init()
