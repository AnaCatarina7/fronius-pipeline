import os
import sys
import threading
import queue
from datetime import datetime
from flask import Flask, render_template, request, jsonify, Response, stream_with_context

app = Flask(__name__)

# ── Queue para capturar logs em tempo real ──────────────────────────────────
log_queue = queue.Queue()
pipeline_running = False


class QueueLogger:
    """Redireciona stdout para a fila de logs."""
    def __init__(self, q):
        self.queue = q
        self.terminal = sys.__stdout__

    def write(self, message):
        if message.strip():
            self.terminal.write(message)
            self.queue.put(message)

    def flush(self):
        self.terminal.flush()


def run_pipeline(start_date: str, days_back: int):
    global pipeline_running
    pipeline_running = True

    # Redireciona prints para a fila
    sys.stdout = QueueLogger(log_queue)

    try:
        log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] A iniciar pipeline...\n")
        log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] start_date={start_date} | days_back={days_back}\n")

        # ── Importa e corre o teu script ───────────────────────────────────
        # Muda o env para "render" (ou "local") conforme o ambiente
        os.environ["ENV"] = os.getenv("ENV", "render")
        os.environ["START_DATE"] = start_date
        os.environ["DAYS_BACK"] = str(days_back)

        # Importa o script principal
        # Se o ficheiro se chama ACTC-Fronius2.py, usa importlib:
        import importlib.util, pathlib
        script_path = pathlib.Path(__file__).parent / "scripts" / "ACTC-Fronius2.py"

        if script_path.exists():
            spec = importlib.util.spec_from_file_location("fronius_pipeline", script_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Pipeline concluído com sucesso!\n")
        else:
            log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️  Script não encontrado em: {script_path}\n")
            log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] Verifica se o caminho está correto no app.py\n")

    except Exception as e:
        log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Erro: {e}\n")
    finally:
        log_queue.put("__DONE__")
        sys.stdout = sys.__stdout__
        pipeline_running = False


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/run", methods=["POST"])
def run():
    global pipeline_running
    if pipeline_running:
        return jsonify({"error": "Pipeline já está em execução."}), 409

    data = request.get_json()
    start_date = data.get("start_date", datetime.today().strftime("%Y-%m-%d"))
    days_back = int(data.get("days_back", 7))

    # Limpa a fila antes de nova execução
    while not log_queue.empty():
        log_queue.get_nowait()

    thread = threading.Thread(target=run_pipeline, args=(start_date, days_back), daemon=True)
    thread.start()
    return jsonify({"status": "started"})


@app.route("/stream")
def stream():
    """Server-Sent Events — envia logs em tempo real para o browser."""
    def generate():
        while True:
            try:
                msg = log_queue.get(timeout=30)
                if msg == "__DONE__":
                    yield f"data: __DONE__\n\n"
                    break
                yield f"data: {msg.rstrip()}\n\n"
            except queue.Empty:
                yield f"data: [timeout] Sem resposta há 30s.\n\n"
                break

    return Response(stream_with_context(generate()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/status")
def status():
    return jsonify({"running": pipeline_running})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)