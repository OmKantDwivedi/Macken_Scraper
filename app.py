from flask import Flask, request, jsonify, send_file, render_template
import uuid
import os
import pandas as pd
from scraper import run_csv
import threading

app = Flask(__name__)

TASKS = {}

@app.route("/")
def home():
    return render_template("index.html")

@app.route('/start', methods=['POST'])
def start_task():
    file = request.files['file']
    task_id = str(uuid.uuid4())

    input_path = f"uploads/{task_id}.csv"
    output_path = f"outputs/{task_id}.csv"

    os.makedirs("uploads", exist_ok=True)
    os.makedirs("outputs", exist_ok=True)

    file.save(input_path)

    TASKS[task_id] = {
        "progress": 0,
        "message": "Starting...",
        "done": False,
        "output": output_path
    }

    threading.Thread(target=run_scraper, args=(task_id, input_path, output_path)).start()

    return jsonify({"taskId": task_id})


def run_scraper(task_id, input_file, output_file):
    df = pd.read_csv(input_file)
    total = len(df)

    TASKS[task_id]["message"] = f"Processing {total} URLs..."

    results = []

    for i, url in enumerate(df["url"]):
        try:
            rows = process_url(url)
            results.extend(rows)
        except:
            pass

        TASKS[task_id]["progress"] = int((i + 1) / total * 100)
        TASKS[task_id]["message"] = f"Processed {i + 1} / {total} URLs"

    pd.DataFrame(results).to_csv(output_file, index=False)

    TASKS[task_id]["done"] = True
    TASKS[task_id]["message"] = "Completed! Click below to download."


@app.route('/status')
def status():
    task_id = request.args.get("taskId")
    t = TASKS.get(task_id)

    if not t:
        return jsonify({"error": "Invalid task ID"})

    return jsonify({
        "progress": t["progress"],
        "message": t["message"],
        "done": t["done"],
        "outputUrl": f"/download/{task_id}" if t["done"] else None
    })


@app.route('/download/<task_id>')
def download(task_id):
    path = TASKS[task_id]["output"]
    return send_file(path, as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True)
