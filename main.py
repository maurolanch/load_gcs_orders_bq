from flask import Flask, request
from snapshot_processor import process_snapshots
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

@app.route("/", methods=["POST"])
def handle_request():
    logging.info("Received POST request.")
    return process_snapshots(request)
