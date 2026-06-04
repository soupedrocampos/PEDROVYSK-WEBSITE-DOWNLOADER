import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import gc
import os
import queue
import shutil
import threading
import time
import uuid

from flask import Flask, Response, jsonify, render_template, request, send_file
from grabber import SiteGrabber, get_site_name, zip_directory

app = Flask(__name__)

DOWNLOAD_FOLDER = 'downloads'
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# Session TTLs (seconds)
COMPLETE_TTL = 1800
ERROR_TTL = 600
ZOMBIE_TTL = 1800
ORPHAN_TTL = 1800
JANITOR_INTERVAL = 300

# Per-session state
_sessions: dict = {}
_queues: dict = {}
_lock = threading.Lock()

# Completed download history (survives session purge)
_history: list = []
_history_lock = threading.Lock()
MAX_HISTORY = 200

# Limit concurrent Playwright browsers to avoid crashing the machine
# Free tier (Render/Railway): 512 MB RAM, ~200-300 MB per Chromium instance.
# Keep at 1 to avoid OOM crashes. Raise to 2-3 only on paid plans (2 GB+ RAM).
MAX_CONCURRENT = int(os.environ.get('MAX_CONCURRENT', '1'))
_semaphore = threading.Semaphore(MAX_CONCURRENT)


# ── Session management ────────────────────────────────────────────────────────

def _purge(session_id: str) -> None:
    with _lock:
        result = _sessions.pop(session_id, None)
        _queues.pop(session_id, None)
    if not result:
        return
    for path in (result.get('zip_path'), os.path.join(DOWNLOAD_FOLDER, session_id)):
        if path and os.path.exists(path):
            try:
                if os.path.isfile(path):
                    os.remove(path)
                else:
                    shutil.rmtree(path, ignore_errors=True)
            except Exception:
                pass


def _janitor() -> None:
    while True:
        time.sleep(JANITOR_INTERVAL)
        try:
            now = time.time()
            to_remove = []

            with _lock:
                snapshot = list(_sessions.items())

            for sid, s in snapshot:
                age = now - (s.get('created_at') or s.get('started_at') or 0)
                status = s.get('status')
                if status == 'complete' and age > COMPLETE_TTL:
                    to_remove.append(sid)
                elif status == 'error' and age > ERROR_TTL:
                    to_remove.append(sid)
                elif status == 'processing' and age > ZOMBIE_TTL:
                    to_remove.append(sid)

            for sid in to_remove:
                _purge(sid)
                print(f"🧹 Sessão {sid[:8]} removida (janitor)")

            # Orphan files
            with _lock:
                known = set(_sessions.keys())
            for entry in os.listdir(DOWNLOAD_FOLDER):
                path = os.path.join(DOWNLOAD_FOLDER, entry)
                base = entry[:-4] if entry.endswith('.zip') else entry
                if base in known:
                    continue
                try:
                    age = now - os.path.getmtime(path)
                except OSError:
                    continue
                if age > ORPHAN_TTL:
                    try:
                        if os.path.isfile(path):
                            os.remove(path)
                        else:
                            shutil.rmtree(path, ignore_errors=True)
                    except Exception:
                        pass

            gc.collect()
        except Exception:
            pass


threading.Thread(target=_janitor, daemon=True).start()


# ── Download worker ───────────────────────────────────────────────────────────

def _worker(session_id: str, url: str) -> None:
    with _lock:
        q = _queues.get(session_id)
    if q is None:
        return

    # Queue if all slots busy
    if not _semaphore.acquire(blocking=False):
        q.put("⏳ Aguardando slot disponível (máx 3 simultâneos)...")
        _semaphore.acquire()

    dl_dir = os.path.join(DOWNLOAD_FOLDER, session_id)
    zip_path = os.path.join(DOWNLOAD_FOLDER, f"{session_id}.zip")
    grabber = None

    try:
        grabber = SiteGrabber(url, dl_dir, log=lambda m: q.put(m))
        ok = grabber.grab()

        if not ok:
            raise RuntimeError("grab() returned False")

        site_name = get_site_name(url)
        q.put("📦 Criando arquivo ZIP...")
        zip_directory(dl_dir, zip_path)

        if os.path.isdir(dl_dir):
            shutil.rmtree(dl_dir, ignore_errors=True)

        q.put("🎉 Download pronto!")
        completed_at = time.time()
        with _lock:
            _sessions[session_id] = {
                'status': 'complete',
                'zip_path': zip_path,
                'filename': f"{site_name}.zip",
                'created_at': completed_at,
            }
        with _history_lock:
            _history.append({
                'url': url,
                'filename': f"{site_name}.zip",
                'completed_at': completed_at,
            })
            if len(_history) > MAX_HISTORY:
                _history.pop(0)

    except Exception as exc:
        q.put(f"❌ Erro: {exc}")
        with _lock:
            _sessions[session_id] = {
                'status': 'error',
                'error': str(exc),
                'created_at': time.time(),
            }
        if os.path.isdir(dl_dir):
            shutil.rmtree(dl_dir, ignore_errors=True)
        if os.path.isfile(zip_path):
            try:
                os.remove(zip_path)
            except Exception:
                pass
    finally:
        _semaphore.release()
        grabber = None
        gc.collect()


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/history')
def history():
    with _history_lock:
        return jsonify(list(reversed(_history)))


@app.route('/health')
def health():
    with _lock:
        info = {'status': 'ok', 'sessions': len(_sessions)}
    try:
        import psutil
        info['rss_mb'] = round(psutil.Process().memory_info().rss / (1024 * 1024), 1)
    except Exception:
        pass
    return jsonify(info)


@app.route('/start-download', methods=['POST'])
def start_download():
    data = request.get_json(silent=True) or {}
    url = (data.get('url') or '').strip()
    if not url:
        return jsonify({'error': 'URL é obrigatória'}), 400

    # Auto-prefix scheme
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url.lstrip('/')

    sid = str(uuid.uuid4())
    with _lock:
        _queues[sid] = queue.Queue()
        _sessions[sid] = {'status': 'processing', 'started_at': time.time()}

    threading.Thread(target=_worker, args=(sid, url), daemon=True).start()
    return jsonify({'session_id': sid})


@app.route('/stream/<session_id>')
def stream(session_id: str):
    def generate():
        with _lock:
            q = _queues.get(session_id)
        if q is None:
            yield "data: ❌ Sessão não encontrada\n\n"
            yield "event: done\ndata: error\n\n"
            return

        deadline = time.time() + 35 * 60  # 35-minute hard cap

        while True:
            if time.time() > deadline:
                yield "data: ⏱️ Tempo esgotado\n\n"
                yield "event: done\ndata: timeout\n\n"
                return
            try:
                msg = q.get(timeout=30)
                yield f"data: {msg}\n\n"
                with _lock:
                    s = _sessions.get(session_id, {})
                if s.get('status') in ('complete', 'error'):
                    yield f"event: done\ndata: {s['status']}\n\n"
                    return
            except queue.Empty:
                with _lock:
                    s = _sessions.get(session_id, {})
                if s.get('status') in ('complete', 'error'):
                    yield f"event: done\ndata: {s['status']}\n\n"
                    return
                yield ": keepalive\n\n"

    return Response(generate(), mimetype='text/event-stream')


@app.route('/download-file/<session_id>')
def download_file(session_id: str):
    with _lock:
        s = _sessions.get(session_id)

    if not s or s.get('status') != 'complete':
        return "Arquivo não disponível", 404

    zip_path = s.get('zip_path')
    filename = s.get('filename')

    if not zip_path or not os.path.exists(zip_path):
        _purge(session_id)
        return "Arquivo não encontrado", 404

    try:
        return send_file(zip_path, as_attachment=True, download_name=filename)
    except Exception as exc:
        return f"Erro ao enviar arquivo: {exc}", 500


if __name__ == '__main__':
    app.run(debug=True, port=4444, threaded=True, use_reloader=False)
else:
    pass
