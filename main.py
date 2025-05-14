from flask import Flask, request
from snapshot_processor import process_snapshots

app = Flask(__name__)

@app.route("/", methods=["POST"])
def handle_request():
    return process_snapshots(request)
