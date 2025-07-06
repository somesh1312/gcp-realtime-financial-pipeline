import base64
import json
from google.cloud import bigquery

def process_transaction(event, context):
    client = bigquery.Client()
    dataset_id = "finance_dataset"
    table_id = "transactions"
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    try:
        if 'data' not in event:
            print("❌ No data found in event.")
            return

        message = base64.b64decode(event['data']).decode('utf-8')
        print("📦 Raw message:", message)

        txn = json.loads(message)

        row_to_insert = [{
            "transaction_id": txn.get("transaction_id"),
            "user_id": txn.get("user_id"),
            "amount": txn.get("amount"),
            "timestamp": txn.get("timestamp"),
            "merchant": txn.get("merchant"),
            "category": txn.get("category"),
            "location": txn.get("location")
        }]

        print("📤 Inserting row:", row_to_insert)
        errors = client.insert_rows_json(table_ref, row_to_insert)

        if errors:
            print("❌ BigQuery insert errors:", errors)
        else:
            print("✅ Row successfully inserted!")

    except Exception as e:
        print("❌ Exception occurred:", e)