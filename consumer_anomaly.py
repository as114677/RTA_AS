from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime
import json

consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers="broker:9092",
    value_deserializer=lambda v: json.loads(v.decode()),
)

okna = defaultdict(deque)

for msg in consumer:
    tx = msg.value
    user = tx["user_id"]
    teraz = datetime.fromisoformat(tx["timestamp"]).timestamp()

    while okna[user] and okna[user][0] < teraz - 60:
        okna[user].popleft()

    okna[user].append(teraz)

    if len(okna[user]) > 3:
        print(f"ALERT! {user} — {len(okna[user])} transakcji w 60s")