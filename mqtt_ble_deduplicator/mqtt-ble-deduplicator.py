from hbmqtt.client import MQTTClient, ConnectException, ClientException
from hbmqtt.mqtt.constants import QOS_0
import os
import json
import asyncio
import sys
import argparse
import collections
from mqtt_ble_deduplicator import dedup

parser = argparse.ArgumentParser(description='Read one MQTT topic, deduplicate messages (compare source address and data content), post deduplicated messages to another topic.')
parser.add_argument('--quiet', dest='quiet', action='store_const',
                    const=True, default=False,
                    help="Quiet mode, don't print messages to stdout")
args = parser.parse_args()

mqtt_url = os.environ.get("MQTT_URL", "mqtt://localhost/")
mqtt_source_topic = os.environ.get("MQTT_SOURCE_TOPIC", "/home/ble")
mqtt_target_topic = os.environ.get("MQTT_TARGET_TOPIC", "/home/ble-deduped")

client = MQTTClient()

dedup_buffers = collections.defaultdict(lambda: collections.defaultdict(lambda: dedup.Deduplicator()))
async def main():
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

            ble_msg = json.loads(data)

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
                # print("Got duplicate from %s, previous receivers: %s, mac: %s" %(message_receiver, duplicate_receiver, mac), content.encode('iso-8859-1').hex())
                pass
            else:
                normals += 1
                # print("Got first instance from %s, mac: %s" %( message_receiver, mac), content.encode('iso-8859-1').hex())
                await client.publish(mqtt_target_topic, data)

            counter += 1

            if counter % 500 == 0:
                print("Processed %d, duplicates: %d" %(counter, duplicates))
        except Exception as e:
            print("MQTT error", e)
        
asyncio.ensure_future(main())
asyncio.get_event_loop().run_forever()
