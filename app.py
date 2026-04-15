import os
import sys
import threading
import queue
from datetime import datetime
from flask import Flask, render_template, request, jsonify, Response, stream_with_context

app = Flask(__name__)

# ── State ─────────────────────────────────────────────────────────────────────
log_queue = queue.Queue()   # holds log messages to stream to the browser
pipeline_running = False    # prevents concurrent pipeline runs


# ── Stdout capture ────────────────────────────────────────────────────────────
class QueueLogger:
    """Redirects stdout to the log queue so every print() reaches the browser."""
    def __init__(self, q):
        self.queue = q
        self.terminal = sys.__stdout__

    def write(self, message):
        if message.strip():
            self.terminal.write(message)
            self.queue.put(message)

    def flush(self):
        self.terminal.flush()


# ── Pipeline runner ───────────────────────────────────────────────────────────
def run_pipeline(start_date: str, days_back: int):
    """Loads and executes the Fronius pipeline script in a background thread."""
    global pipeline_running
    pipeline_running = True
    sys.stdout = QueueLogger(log_queue)

    try:
        log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] A iniciar pipeline...\n")
        log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] start_date={start_date} | days_back={days_back}\n")

        # Pass parameters to the pipeline script via environment variables
        os.environ["START_DATE"] = start_date
        os.environ["DAYS_BACK"]  = str(days_back)

        # Load and execute the pipeline script dynamically (avoids refactoring it)
        import importlib.util, pathlib
        script_path = pathlib.Path(__file__).parent / "scripts" / "ACTC-Fronius2.py"

        if script_path.exists():
            spec   = importlib.util.spec_from_file_location("fronius_pipeline", script_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Pipeline concluído com sucesso!\n")
        else:
            log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️  Script not found at: {script_path}\n")

    except Exception as e:
        log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Error: {e}\n")

    finally:
        log_queue.put("__DONE__")
        sys.stdout = sys.__stdout__
        pipeline_running = False


# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    """Serves the main control panel."""
    return render_template("index.html")


@app.route("/run", methods=["POST"])
def run():
    """Starts the pipeline in a background thread. Rejects concurrent runs."""
    global pipeline_running
    if pipeline_running:
        return jsonify({"error": "Pipeline já está em execução."}), 409

    data       = request.get_json()
    start_date = data.get("start_date", datetime.today().strftime("%Y-%m-%d"))
    days_back  = int(data.get("days_back", 7))

    # Clear any leftover messages from a previous run
    while not log_queue.empty():
        log_queue.get_nowait()

    thread = threading.Thread(target=run_pipeline, args=(start_date, days_back), daemon=True)
    thread.start()
    return jsonify({"status": "started"})


@app.route("/stream")
def stream():
    """Server-Sent Events endpoint — pushes log lines to the browser in real time."""
    def generate():
        while True:
            try:
                msg = log_queue.get(timeout=30)
                if msg == "__DONE__":
                    yield "data: __DONE__\n\n"
                    break
                yield f"data: {msg.rstrip()}\n\n"
            except queue.Empty:
                yield "data: [timeout] Sem resposta há 30s.\n\n"
                break

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@app.route("/status")
def status():
    """Returns whether the pipeline is currently running (used by the frontend)."""
    return jsonify({"running": pipeline_running})


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # PORT is injected by Render in production; defaults to 5000 locally
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)