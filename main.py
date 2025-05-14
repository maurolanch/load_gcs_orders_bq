import os
import json
import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage, bigquery
import logging

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "lanch-pipeline-v3-2774240a502b.json"

BUCKET_NAME = 'ml-orders-snapshots'
PREFIX = 'mercadolibre/order_snapshots/'                         
BQ_TABLE = 'lanch-pipeline-v3.lanch_staging.order_snapshots'

storage_client = storage.Client()
bq_client = bigquery.Client()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bucket = storage_client.bucket(BUCKET_NAME)
blobs = list(bucket.list_blobs(prefix=PREFIX))

def extract_snapshot_from_blob(blob):
    try:
        content = blob.download_as_text()
        if not content.strip():
            raise ValueError(f"Archivo vacío: {blob.name}")
        obj = json.loads(content)
        if obj is None:
            raise ValueError(f"JSON inválido: {blob.name}")
        cancel = obj.get("cancel_detail") or {}
        return {
            'order_id': str(obj.get("id", None)),
            'snapshot_ts': datetime.now(timezone.utc),
            'date_created': obj.get("date_created", None),
            'last_updated': obj.get("last_updated", None),
            'date_closed': obj.get("date_closed", None),
            'fulfilled': obj.get("fulfilled", None),
            'total_amount': obj.get("total_amount", None),
            'paid_amount': obj.get("paid_amount", None),
            'currency_id': obj.get("currency_id", None),
            'status': obj.get("status", None),
            'tags': obj.get("tags", []),
            'cancel_detail_group': cancel.get("group", None),
            'cancel_detail_code': cancel.get("code", None),
            'cancel_detail_desc': cancel.get("description", None),
            'cancel_detail_date': cancel.get("date", None),
        }
    except json.JSONDecodeError as e:
        logger.error(f"JSON malformado en {blob.name}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error procesando {blob.name}: {e}")
        return None

def main():
    snapshots = []
    blob = blobs[0]
    logger.info(f"Procesando archivo: {blob.name}")
    snapshot = extract_snapshot_from_blob(blob)
    if snapshot:
        snapshots.append(snapshot)
    if not snapshots:
        logger.warning("No se procesó ningún snapshot válido.")
        return
    df = pd.DataFrame(snapshots)
    datetime_cols = [
        'snapshot_ts', 'date_created', 'last_updated',
        'date_closed', 'cancel_detail_date'
    ]
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)
    try:
        logger.info(f"Cargando datos a BigQuery: {BQ_TABLE}")
        job = bq_client.load_table_from_dataframe(df, BQ_TABLE)
        job.result()
        logger.info("Carga completada exitosamente.")
    except Exception as e:
        logger.error(f"Error cargando datos a BigQuery: {e}")

if __name__ == "__main__":
    main()
