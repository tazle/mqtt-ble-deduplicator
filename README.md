
# Build

`docker build -t mqtt-ble-deduplicator`

# Run

`docker run --rm -it -e MQTT_URL=mqtt://mqtt-host/ mqtt-ble-deduplicator:latest`

# Run tests
`PYTHONPATH=$PWD py.test`