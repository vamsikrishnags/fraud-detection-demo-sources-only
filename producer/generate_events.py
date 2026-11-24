"""Generates synthetic transaction, login and account-change events.

Reads connection details from bob-config.json (written by `uv run deploy`).

Cost-aware by design: the LLM downstream is only expensive when suspicious
activity reaches it. So by default this streams continuous *normal* activity and
injects one fraud scenario every --fraud-every cycles (default 10, ~4 minutes).
Use --fraud-every 0 for normal-only traffic (zero fraud signals), or fire a
single scenario on demand with --inject.
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "bob-config.json")


def load_config(path=CONFIG_PATH):
    """Connection settings from bob-config.json; empty values if it doesn't exist yet."""
    try:
        with open(path, encoding="utf-8") as f:
            cfg = json.load(f)
    except (OSError, ValueError):
        return {}
    kafka, sr = cfg.get("kafka", {}), cfg.get("schema_registry", {})
    return {
        "bootstrap": kafka.get("bootstrap_servers", ""),
        "kafka_key": kafka.get("api_key", ""),
        "kafka_secret": kafka.get("api_secret", ""),
        "sr_url": sr.get("url", ""),
        "sr_key": sr.get("api_key", ""),
        "sr_secret": sr.get("api_secret", ""),
    }


_CFG = load_config()
BOOTSTRAP = _CFG.get("bootstrap", "")
KAFKA_API_KEY = _CFG.get("kafka_key", "")
KAFKA_API_SECRET = _CFG.get("kafka_secret", "")
SR_URL = _CFG.get("sr_url", "")
SR_API_KEY = _CFG.get("sr_key", "")
SR_API_SECRET = _CFG.get("sr_secret", "")

# Topic names match the Flink tables created by Terraform (see bob-config.json).
TOPIC_TRANSACTIONS = "transactions"
TOPIC_LOGINS = "user_logins"
TOPIC_ACCOUNT_CHANGES = "account_changes"

# Avro value schemas matching the Flink-generated schemas (record name
# "<topic>_value", namespace org.apache.flink.avro.generated.record, field order
# matching the CREATE TABLE column order). The serializer uses the schema already
# registered by Flink (use.latest.version), so these are mainly documentation.
NAMESPACE = "org.apache.flink.avro.generated.record"

TRANSACTION_SCHEMA = """{
  "type": "record", "name": "transactions_value", "namespace": "%s",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "transaction_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "merchant", "type": "string"},
    {"name": "merchant_category", "type": "string"},
    {"name": "location", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}""" % NAMESPACE

LOGIN_SCHEMA = """{
  "type": "record", "name": "user_logins_value", "namespace": "%s",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "ip_address", "type": "string"},
    {"name": "device_id", "type": "string"},
    {"name": "location", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}""" % NAMESPACE

ACCOUNT_CHANGE_SCHEMA = """{
  "type": "record", "name": "account_changes_value", "namespace": "%s",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "field_changed", "type": "string"},
    {"name": "old_value", "type": "string"},
    {"name": "new_value", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}""" % NAMESPACE

USERS = [f"user-{i:03d}" for i in range(1, 11)]
DEVICES = ["iphone-15", "pixel-8", "macbook-pro", "windows-desktop", "ipad-air"]
MERCHANTS = [
    ("Starbucks", "food_and_drink"),
    ("Amazon", "online_retail"),
    ("Walmart", "grocery"),
    ("Shell Gas", "fuel"),
    ("Netflix", "subscription"),
    ("Best Buy", "electronics"),
    ("Target", "retail"),
    ("Uber", "transportation"),
]
CITIES = [
    ("San Francisco, CA", "198.51.100.1"),
    ("New York, NY", "203.0.113.10"),
    ("Tokyo, Japan", "192.0.2.50"),
    ("London, UK", "198.51.100.99"),
    ("Chicago, IL", "203.0.113.42"),
    ("Sydney, Australia", "192.0.2.100"),
]


def now_ms():
    return int(time.time() * 1000)


def delivery_report(err, msg):
    if err is not None:
        print(f"  ERROR: Message delivery failed: {err}")


def build_serializers():
    sr = SchemaRegistryClient({
        "url": SR_URL,
        "basic.auth.user.info": f"{SR_API_KEY}:{SR_API_SECRET}",
    })
    # Serialize against the schema Flink already registered for each subject.
    conf = {"auto.register.schemas": False, "use.latest.version": True}
    return {
        TOPIC_TRANSACTIONS: AvroSerializer(sr, TRANSACTION_SCHEMA, conf=conf),
        TOPIC_LOGINS: AvroSerializer(sr, LOGIN_SCHEMA, conf=conf),
        TOPIC_ACCOUNT_CHANGES: AvroSerializer(sr, ACCOUNT_CHANGE_SCHEMA, conf=conf),
    }


def make_producer():
    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": KAFKA_API_KEY,
        "sasl.password": KAFKA_API_SECRET,
        "client.id": "datagen-bob-producer",
    })


_key_serializer = StringSerializer("utf_8")


def produce_event(producer, serializers, topic, event):
    value = serializers[topic](event, SerializationContext(topic, MessageField.VALUE))
    key = _key_serializer(event["user_id"], SerializationContext(topic, MessageField.KEY))
    producer.produce(topic=topic, key=key, value=value, callback=delivery_report)
    producer.poll(0)


def make_transaction(user_id, amount=None, merchant=None, location=None):
    m = merchant or random.choice(MERCHANTS)
    loc = location or random.choice(CITIES[:3])
    return {
        "user_id": user_id,
        "transaction_id": str(uuid.uuid4())[:8],
        "amount": amount or round(random.uniform(3, 150), 2),
        "merchant": m[0],
        "merchant_category": m[1],
        "location": loc[0],
        "timestamp": now_ms(),
    }


def make_login(user_id, location=None, device=None):
    loc = location or random.choice(CITIES[:3])
    return {
        "user_id": user_id,
        "ip_address": loc[1],
        "device_id": device or random.choice(DEVICES[:3]),
        "location": loc[0],
        "timestamp": now_ms(),
    }


def make_account_change(user_id, field="preferences", old_val="default", new_val="custom"):
    return {
        "user_id": user_id,
        "field_changed": field,
        "old_value": old_val,
        "new_value": new_val,
        "timestamp": now_ms(),
    }


def normal_activity(producer, serializers, user_id):
    """Normal user: login from one city, 1-2 small purchases nearby."""
    city = random.choice(CITIES[:3])

    login = make_login(user_id, location=city)
    produce_event(producer, serializers, TOPIC_LOGINS, login)
    print(f"  [normal] {user_id} login from {city[0]}")

    time.sleep(random.uniform(0.5, 2))

    for _ in range(random.randint(1, 2)):
        txn = make_transaction(user_id, amount=round(random.uniform(5, 80), 2), location=city)
        produce_event(producer, serializers, TOPIC_TRANSACTIONS, txn)
        print(f"  [normal] {user_id} txn ${txn['amount']:.2f} at {txn['merchant']} in {city[0]}")
        time.sleep(random.uniform(0.3, 1))


def fraud_geo_impossible(producer, serializers, user_id):
    """Login from Tokyo, purchase from NYC seconds later."""
    login = make_login(user_id, location=CITIES[2])  # Tokyo
    produce_event(producer, serializers, TOPIC_LOGINS, login)
    print(f"  [FRAUD:geo] {user_id} login from Tokyo")

    time.sleep(0.5)

    txn = make_transaction(
        user_id,
        amount=round(random.uniform(500, 2000), 2),
        merchant=("Best Buy", "electronics"),
        location=CITIES[1],  # New York
    )
    produce_event(producer, serializers, TOPIC_TRANSACTIONS, txn)
    print(f"  [FRAUD:geo] {user_id} txn ${txn['amount']:.2f} in New York (impossible travel)")


def fraud_account_takeover(producer, serializers, user_id):
    """Email change -> password change -> large purchase."""
    change1 = make_account_change(user_id, "email", "user@old.com", "hacker@temp.com")
    produce_event(producer, serializers, TOPIC_ACCOUNT_CHANGES, change1)
    print(f"  [FRAUD:takeover] {user_id} email changed")

    time.sleep(0.5)

    change2 = make_account_change(user_id, "password", "****", "****")
    produce_event(producer, serializers, TOPIC_ACCOUNT_CHANGES, change2)
    print(f"  [FRAUD:takeover] {user_id} password changed")

    time.sleep(0.5)

    txn = make_transaction(
        user_id,
        amount=round(random.uniform(1000, 5000), 2),
        merchant=("Amazon", "online_retail"),
    )
    produce_event(producer, serializers, TOPIC_TRANSACTIONS, txn)
    print(f"  [FRAUD:takeover] {user_id} large purchase ${txn['amount']:.2f} after credential changes")


def fraud_velocity(producer, serializers, user_id):
    """Many rapid transactions across different merchants."""
    login = make_login(user_id, location=CITIES[0])
    produce_event(producer, serializers, TOPIC_LOGINS, login)
    print(f"  [FRAUD:velocity] {user_id} login from {CITIES[0][0]}")

    time.sleep(0.3)

    for i in range(6):
        txn = make_transaction(
            user_id,
            amount=round(random.uniform(200, 800), 2),
            merchant=MERCHANTS[i % len(MERCHANTS)],
        )
        produce_event(producer, serializers, TOPIC_TRANSACTIONS, txn)
        print(f"  [FRAUD:velocity] {user_id} rapid txn #{i+1} ${txn['amount']:.2f} at {txn['merchant']}")
        time.sleep(0.2)


FRAUD_SCENARIOS = {
    "geo": fraud_geo_impossible,
    "takeover": fraud_account_takeover,
    "velocity": fraud_velocity,
}


def main():
    parser = argparse.ArgumentParser(description="Stream synthetic activity to Confluent Cloud, with controlled fraud")
    parser.add_argument(
        "--fraud-every",
        type=int,
        default=10,
        metavar="N",
        help="inject one fraud scenario every N cycles (default 10, about 4 minutes); 0 = normal traffic only",
    )
    parser.add_argument(
        "--inject",
        choices=sorted(FRAUD_SCENARIOS),
        help="send ONE fraud scenario now and exit (on-demand trigger for the demo)",
    )
    parser.add_argument("--user", default="user-007", help="user id for --inject (default user-007)")
    args = parser.parse_args()
    if args.fraud_every < 0:
        parser.error("--fraud-every must be >= 0")
    if not all((BOOTSTRAP, KAFKA_API_KEY, KAFKA_API_SECRET, SR_URL, SR_API_KEY, SR_API_SECRET)):
        parser.error("Missing connection configuration. Run `uv run deploy` first to create bob-config.json.")
    print(f"Connecting to Confluent Cloud at {BOOTSTRAP}...")
    producer = make_producer()
    serializers = build_serializers()

    if args.inject:
        FRAUD_SCENARIOS[args.inject](producer, serializers, args.user)
        producer.flush()
        print(f"\nInjected one '{args.inject}' fraud scenario for {args.user}.")
        return

    fraud_note = f"fraud scenario every {args.fraud_every} cycles" if args.fraud_every else "normal traffic only"
    print(f"Connected. Generating events: {fraud_note} (Ctrl+C to stop).\n")

    cycle = 0
    while True:
        cycle += 1
        print(f"--- Cycle {cycle} ({datetime.now().strftime('%H:%M:%S')}) ---")

        random.shuffle(USERS)

        for user_id in USERS[:6]:
            normal_activity(producer, serializers, user_id)

        if args.fraud_every and cycle % args.fraud_every == 0:
            fraud_user = random.choice(USERS[6:])
            fraud_fn = random.choice(list(FRAUD_SCENARIOS.values()))
            fraud_fn(producer, serializers, fraud_user)

        producer.flush()
        wait = random.uniform(5, 15)
        print(f"\nWaiting {wait:.0f}s before next cycle...\n")
        time.sleep(wait)


if __name__ == "__main__":
    main()
