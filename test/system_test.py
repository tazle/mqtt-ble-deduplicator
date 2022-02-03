import os
import subprocess
mbd = __import__('mqtt_ble_deduplicator.mqtt-ble-deduplicator')
system = getattr(mbd, 'mqtt-ble-deduplicator')
import pytest

import paho.mqtt.client as mqtt
import multiprocessing
import time
import json

TEST_TOPIC = "/test"
WAIT_SECONDS = 5

def gen_mac(i):
    gen_mac_ending = hex(i).rjust(4, "0")
    gen_receiver_mac = "00:00:00:00" + gen_mac_ending[:2] + ":" + gen_mac_ending[2:]
    return gen_receiver_mac

@pytest.mark.skipif(os.environ.get("HAVE_MQTT", None) is None, reason="Need MQTT environment")
def test_dockerized_dedupe_on_message_removes_duplicates(docker_image, mqtt_broker):
    hostname, port = mqtt_broker

    received_dedupes = []
    test_messages = []
    def on_connect(client, userdata, flags, rc):
        print("Tester connected")
        client.subscribe(system.mqtt_target_topic)
        client.subscribe(TEST_TOPIC)

    def on_message(client, userdata, msg):
        if msg.topic == system.mqtt_target_topic:
            received_dedupes.append(msg)
        elif msg.topic == TEST_TOPIC:
            test_messages.append(msg)
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.loop_start()
        client.connect(hostname, port, 60)

        for i in range(WAIT_SECONDS * 10):
            if test_messages:
                print("Tester got message through broker", test_messages[0])
                break
            client.publish(TEST_TOPIC, payload="foo")
            time.sleep(.1)
        else:
            pytest.fail("Tester could not pass message through broker")

        time.sleep(5)
        mqtt_url = "mqtt://mqtt:1883"
        cmd = ["docker", "run", "--rm", "--link", "test_vernemq:mqtt", "-e", "MQTT_URL=" + mqtt_url, docker_image]
        print(" ".join(cmd))
        dedupe = multiprocessing.Process(target=lambda: subprocess.check_output(cmd), daemon=True)
        dedupe.start()

        payload = json.loads(r'{"uuid16s": [], "address": {"address": "FB:72:49:EA:C7:4A"}, "svc_data_uuid32": null, "uuid32s": [], "service_data": null, "svc_data_uuid128": null, "rssi": null, "type": "ADV_NONCONN_IND", "uri": null, "adv_itvl": null, "svc_data_uuid16": null, "uuid128s": [], "receiver_mac": "B8:27:EB:8E:F1:12", "tx_pwr_lvl": null, "raw_data": null, "appearance": null, "address_type": "RANDOM", "public_tgt_addr": null, "_name": null, "flags": 6, "name_is_complete": false, "mfg_data": "\u0099\u0004\u0003\u00c6\u0001?\u00ca\u00dd\u00ff\u00e8\u0000\u0015\u0003\u00fe\u000b;"}')


        first_message_at = None
        for i in range(WAIT_SECONDS * 10):
            gen_receiver_mac = gen_mac(i)
            payload['receiver_mac'] = gen_receiver_mac
            client.publish(system.mqtt_source_topic, json.dumps(payload))
            if len(received_dedupes) != 0:
                if first_message_at is None:
                    first_message_at = time.time()
                if (time.time() - first_message_at) > 0.5:
                    if len(received_dedupes) != 1:
                        print("Got duplicates", received_dedupes)
                        pytest.fail("Received duplicates")
                    else:
                        break
            time.sleep(.1)
        else:
            pytest.fail("Tester did not receive deduplicated message")
    finally:
        client.loop_stop()
        dedupe.kill()



@pytest.mark.skipif(os.environ.get("HAVE_MQTT", None) is None, reason="Need MQTT environment")
def test_dedupe_on_message_removes_duplicates(mqtt_broker):
    hostname, port = mqtt_broker
    
    received_dedupes = []
    test_messages = []
    def on_connect(client, userdata, flags, rc):
        print("Tester connected")
        client.subscribe(system.mqtt_target_topic)
        client.subscribe(TEST_TOPIC)
        
    def on_message(client, userdata, msg):
        if msg.topic == system.mqtt_target_topic:
            received_dedupes.append(msg)
        elif msg.topic == TEST_TOPIC:
            test_messages.append(msg)
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.loop_start()
        client.connect(hostname, port, 60)

        for i in range(WAIT_SECONDS * 10):
            if test_messages:
                print("Tester got message through broker", test_messages[0])
                break
            client.publish(TEST_TOPIC, payload="foo")
            time.sleep(.1)
        else:
            pytest.fail("Tester could not pass message through broker")
        dedupe = multiprocessing.Process(target=lambda: system.init(), daemon=True)
        dedupe.start()

        payload = json.loads(r'{"uuid16s": [], "address": {"address": "FB:72:49:EA:C7:4A"}, "svc_data_uuid32": null, "uuid32s": [], "service_data": null, "svc_data_uuid128": null, "rssi": null, "type": "ADV_NONCONN_IND", "uri": null, "adv_itvl": null, "svc_data_uuid16": null, "uuid128s": [], "receiver_mac": "B8:27:EB:8E:F1:12", "tx_pwr_lvl": null, "raw_data": null, "appearance": null, "address_type": "RANDOM", "public_tgt_addr": null, "_name": null, "flags": 6, "name_is_complete": false, "mfg_data": "\u0099\u0004\u0003\u00c6\u0001?\u00ca\u00dd\u00ff\u00e8\u0000\u0015\u0003\u00fe\u000b;"}')
        
        
        first_message_at = None
        for i in range(WAIT_SECONDS * 10):
            gen_receiver_mac = gen_mac(i)
            payload['receiver_mac'] = gen_receiver_mac
            client.publish(system.mqtt_source_topic, json.dumps(payload))
            if len(received_dedupes) != 0:
                if first_message_at is None:
                    first_message_at = time.time()
                if (time.time() - first_message_at) > 0.5:
                    if len(received_dedupes) != 1:
                        print("Got duplicates", received_dedupes)
                        pytest.fail("Received duplicates")
                    else:
                        break
            time.sleep(.1)
        else:
            pytest.fail("Tester did not receive deduplicated message")
    finally:
        client.loop_stop()
        dedupe.kill()

@pytest.fixture(scope="function")
def docker_image():
    import random
    n = random.randint(0, 10000)
    tag = "test-" + str(n)
    image = subprocess.check_output(["docker", "build", ".", "-t", tag])
    return tag

@pytest.fixture(scope="function")
def mqtt_broker(monkeypatch):
    vernemq = multiprocessing.Process(target=lambda: subprocess.run(["docker", "run", "--rm", "--name", "test_vernemq", "-e", "DOCKER_VERNEMQ_ALLOW_ANONYMOUS=on", "-e", "DOCKER_VERNEMQ_ACCEPT_EULA=yes", "-p", "1883", "vernemq/vernemq:1.12.3"], check=True), daemon=True)
    vernemq.start()
    while not 'test_vernemq' in subprocess.check_output(["docker", "ps"]).decode("utf-8"):
        print("Tester waiting for VerneMQ to start")
        time.sleep(1)
    port = subprocess.check_output(["docker", "port", "test_vernemq", "1883"]).decode("utf-8").split("\n")[0].split(":")[1]
    monkeypatch.setenv("MQTT_URL", "mqtt://localhost:" + port + "/")
    yield "localhost", int(port)
    subprocess.run(["docker", "kill", "test_vernemq"])
    vernemq.kill()
