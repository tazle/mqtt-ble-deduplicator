FROM python:3.9

COPY requirements.txt /
RUN pip install -r requirements.txt

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/

RUN mkdir -p mqtt_ble_deduplicator
COPY mqtt_ble_deduplicator/*.py /mqtt_ble_deduplicator/
CMD python3 /mqtt_ble_deduplicator/mqtt-ble-deduplicator.py
