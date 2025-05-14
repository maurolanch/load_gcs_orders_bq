import os
import json
import logging
from datetime import datetime, timedelta, timezone
import pandas as pd
from google.cloud import storage, bigquery

BUCKET_NAME = os.getenv("BUCKET_NAME")
PREFIX = os.getenv("PREFIX")
BQ_TABLE = os.getenv("BQ_TABLE")

storage_client = storage.Client()
bq_client = bigquery.Client()

def extract_snapshot_from_blob(blob):
    try:
        content = blob.download_as_text()
        if not content.strip():
            logging.warning(f"Blob {blob.name} is empty.")
            return None

        obj = json.loads(content)
        if obj is None:
            logging.warning(f"Blob {blob.name} contains invalid JSON.")
            return None

        cancel = obj.get("cancel_detail") or {}

        return {
            'order_id': str(obj.get("id")),
            'snapshot_ts': datetime.now(timezone.utc),
            'date_created': obj.get("date_created"),
            'last_updated': obj.get("last_updated"),
            'date_closed': obj.get("date_closed"),
            'fulfilled': obj.get("fulfilled"),
            'total_amount': obj.get("total_amount"),
            'paid_amount': obj.get("paid_amount"),
            'currency_id': obj.get("currency_id"),
            'status': obj.get("status"),
            'tags': obj.get("tags", []),
            'cancel_detail_group': cancel.get("group"),
            'cancel_detail_code': cancel.get("code"),
            'cancel_detail_desc': cancel.get("description"),
            'cancel_detail_date': cancel.get("date"),
        }
    except Exception as e:
        logging.error(f"Failed to process blob {blob.name}: {e}")
        return None

def process_snapshots(request):
    logging.info("Starting snapshot extraction job.")

    target_date = datetime.now(timezone.utc) - timedelta(days=1)
    
    prefix = f"{PREFIX}year={target_date.strftime('%Y')}/month={target_date.strftime('%m')}/day={target_date.strftime('%d')}/"

    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            logging.info(f"No blobs found for prefix {prefix}.")
            return "No files to process", 200

        snapshots = []
        for blob in blobs:
            snapshot = extract_snapshot_from_blob(blob)
            if snapshot:
                snapshots.append(snapshot)

        if not snapshots:
            logging.info("No valid snapshots found.")
            return "No valid data found", 200

        df = pd.DataFrame(snapshots)

        datetime_cols = [
            'snapshot_ts', 'date_created', 'last_updated',
            'date_closed', 'cancel_detail_date'
        ]

        for col in datetime_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.tz_localize(None)

        job = bq_client.load_table_from_dataframe(df, BQ_TABLE)
        job.result()

        logging.info(f"Successfully loaded {len(df)} rows to BigQuery.")
        return f"Loaded {len(df)} rows to BigQuery", 200

    except Exception as e:
        logging.error(f"Error during snapshot processing: {e}")
        return "Internal server error", 500
