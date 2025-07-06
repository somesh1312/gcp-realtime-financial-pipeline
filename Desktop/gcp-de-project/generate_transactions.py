import json
import random
from datetime import datetime
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("smart-caster-465101-r6", "transactions-stream")

def generate_transaction(request):  # HTTP-triggered
    merchants = ['Amazon', 'Walmart', 'Starbucks', 'Apple', 'Netflix']
    categories = ['Retail', 'Grocery', 'Food', 'Tech', 'Entertainment']
    locations = ["New York", "Dallas", "San Francisco", "Chicago", "Austin"]

    for _ in range(1000):  # send 10 txns per trigger
        txn = {
            "transaction_id": f"T{random.randint(1000, 9999)}",
            "user_id": f"U{random.randint(100, 999)}",
            "amount": round(random.uniform(5.0, 1500.0), 2),
            "timestamp": datetime.utcnow().isoformat(),
            "merchant": random.choice(merchants),
            "category": random.choice(categories),
            "location": random.choice(locations)
        }

        data = json.dumps(txn).encode("utf-8")
        publisher.publish(topic_path, data)

    return "Published 10 transactions"