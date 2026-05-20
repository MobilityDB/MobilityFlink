"""
BerlinMOD Kafka producer for the MobilityFlink BerlinMOD-Q3 scaffold.

Reads a BerlinMOD CSV (produced by the BerlinMOD generator —
`meos/examples/data/generate_berlinmod_trips.sql` in MobilityDB, at any
scale factor) and emits one JSON record per row to the Kafka topic
`berlinmod`.

Expected CSV columns (in order):
    t              -- "YYYY-MM-DD HH:MM:SS"
    vehicle_id     -- int
    lon            -- float
    lat            -- float

Companion of `python-producer.py` (the existing AIS producer) — same
shape, different schema and topic.
"""

from json import dumps
from kafka import KafkaProducer
import pandas as pd

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "berlinmod"
CSV_PATH = "berlinmod-trips.csv"


def gen_data():
    df = pd.read_csv(CSV_PATH)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: dumps(x).encode("utf-8"),
    )
    for _, row in df.iterrows():
        record = {
            "t": row["t"],
            "vehicle_id": int(row["vehicle_id"]),
            "lon": float(row["lon"]),
            "lat": float(row["lat"]),
        }
        producer.send(topic=TOPIC, value=record)
    producer.flush()


if __name__ == "__main__":
    gen_data()
