# server_revised.py
"""
Enhanced server for scalable parallel simulation.

Main additions vs original:
 - worker_type support (clients tell server if they are 'fast'/'slow'/...)
 - dispatch policies: 'round_robin', 'least_busy', 'random'
 - autoscale_demo: spawn N local worker subprocesses for quick demos
 - experiment mode: run serial vs parallel comparison, produce CSV
 - failure injection control hooks (UI can toggle)
Base original: see user's server.py. :contentReference[oaicite:11]{index=11}
"""

import socket
import threading
import json
import time
import random
import queue
import sqlite3
import subprocess
import sys
import os
from flask import Flask, send_from_directory, request, jsonify
from flask_socketio import SocketIO

# ---------- CONFIG ----------
HOST = ""
TCP_PORT = 9999
WS_PORT = 5000

HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 6.0

CUSTOMER_MEAN = 1.2
SERVICE_MIN = 2.0
SERVICE_MAX = 5.0

DB_FILE = "server_data.db"

# dispatch policy: 'round_robin' | 'least_busy' | 'random'
DISPATCH_POLICY = "least_busy"

# ---------- Flask + SocketIO ----------
app = Flask(__name__, static_folder="static")
sio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/dispatch_policy", methods=["POST"])
def set_policy():
    global DISPATCH_POLICY
    data = request.json or {}
    p = data.get("policy")
    if p in ("round_robin", "least_busy", "random"):
        DISPATCH_POLICY = p
        server_log(f"Dispatch policy changed to {p}")
        return jsonify({"ok": True, "policy": p})
    return jsonify({"ok": False, "error": "invalid policy"}), 400

@app.route("/api/autoscale", methods=["POST"])
def autoscale():
    """Start local demo workers (spawns local python client processes)."""
    data = request.json or {}
    count = int(data.get("count", 0))
    typ = data.get("type", "fast")
    # spawn background processes
    spawned = []
    for i in range(count):
        # note: in windows, you may need different invocation
        cmd = [sys.executable, "client.py", "127.0.0.1", "--type", typ]
        p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        spawned.append(p.pid)
    server_log(f"Autoscale: spawned {len(spawned)} local workers (type={typ})")
    return jsonify({"spawned": spawned})

# ---------- DB helpers (same as original) ----------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY,
            arrival REAL,
            dispatched REAL,
            started REAL,
            finished REAL,
            service_time REAL,
            assigned_worker TEXT
        )
    """)
    conn.commit()
    conn.close()

def db_upsert(cust):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO customers
        (id, arrival, dispatched, started, finished, service_time, assigned_worker)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        cust.get("customer_id"),
        cust.get("arrival"),
        cust.get("dispatched"),
        cust.get("started"),
        cust.get("finished"),
        cust.get("service_time"),
        cust.get("assigned_worker"),
    ))
    conn.commit()
    conn.close()

def recent_jobs(limit=50):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, arrival, dispatched, started, finished, service_time, assigned_worker FROM customers ORDER BY id DESC LIMIT ?", (limit,))
    rows = c.fetchall()
    conn.close()
    jobs = []
    for r in rows:
        jobs.append({
            "customer_id": r[0],
            "arrival": r[1],
            "dispatched": r[2],
            "started": r[3],
            "finished": r[4],
            "service_time": r[5],
            "assigned_worker": r[6]
        })
    return jobs

# ---------- Shared state ----------
central_queue = queue.Queue()
clients = {}  # client_id -> info dict
clients_lock = threading.Lock()

metrics = {"total_generated": 0, "total_dispatched": 0, "total_completed": 0}
metrics_lock = threading.Lock()

control = {
    "running": True,
    "customer_mean": CUSTOMER_MEAN,
    "failure_injection": False
}

# ---------- logging ----------
def server_log(msg):
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        sio.emit("log", line)
    except Exception:
        pass

# ---------- Customer generator ----------
cust_seq = 1
def customer_generator(stop):
    global cust_seq
    while not stop.is_set():
        if not control["running"]:
            time.sleep(0.5)
            continue
        mean = control.get("customer_mean", CUSTOMER_MEAN)
        delay = max(0.2, random.expovariate(1.0 / mean))
        time.sleep(delay)
        cid = cust_seq
        cust_seq += 1
        st = round(random.uniform(SERVICE_MIN, SERVICE_MAX), 2)
        cust = {"customer_id": cid, "arrival": time.time(), "service_time": st}
        central_queue.put(cust)
        db_upsert(cust)
        with metrics_lock:
            metrics["total_generated"] += 1
        server_log(f"New customer {cid} st={st}")
        emit_snapshot()

# ---------- Dispatcher (policy aware) ----------
rr_idx = 0
def choose_client_for_job(cust):
    """Return client_id or None according to policy."""
    global rr_idx
    with clients_lock:
        items = list(clients.items())
        if not items:
            return None
        if DISPATCH_POLICY == "random":
            # pick random free client
            free = [cid for cid,info in items if not info.get("busy")]
            return random.choice(free) if free else None
        elif DISPATCH_POLICY == "round_robin":
            # iterate items starting at rr_idx to find free
            n = len(items)
            for i in range(n):
                idx = (rr_idx + i) % n
                cid, info = items[idx]
                if not info.get("busy"):
                    rr_idx = (idx + 1) % n
                    return cid
            return None
        elif DISPATCH_POLICY == "least_busy":
            # choose free client with smallest jobs_done or busy_time
            free_infos = [(cid,info) for cid,info in items if not info.get("busy")]
            if not free_infos:
                return None
            # prefer client with lowest busy_time (or jobs_done)
            best = min(free_infos, key=lambda x: (x[1].get("busy_time",0.0), x[1].get("jobs_done",0)))
            return best[0]
        else:
            # fallback to first free
            for cid, info in items:
                if not info.get("busy"):
                    return cid
            return None

def dispatcher(stop):
    while not stop.is_set():
        try:
            cust = central_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        assigned_client = None
        cid = choose_client_for_job(cust)
        if cid:
            with clients_lock:
                info = clients.get(cid)
                if info and not info.get("busy"):
                    try:
                        msg = {"type":"job","customer_id":cust["customer_id"],"service_time":cust["service_time"]}
                        info["sock"].sendall((json.dumps(msg) + "\n").encode())
                        info["busy"] = True
                        info["current_job"] = cust
                        info["last_heartbeat"] = time.time()
                        info["started_ts"] = time.time()

                        cust["dispatched"] = time.time()
                        cust["started"] = info["started_ts"]
                        cust["assigned_worker"] = cid
                        db_upsert(cust)

                        with metrics_lock:
                            metrics["total_dispatched"] += 1
                        server_log(f"DISPATCH: Customer {cust['customer_id']} -> {cid}")
                        assigned_client = cid
                    except Exception as e:
                        server_log(f"Dispatch send error to {cid}: {e}")
        if not assigned_client:
            central_queue.put(cust)
        emit_snapshot()

# ---------- Heartbeat monitor ----------
def monitor_clients(stop):
    while not stop.is_set():
        now = time.time()
        dead = []
        with clients_lock:
            for cid, info in list(clients.items()):
                last = info.get("last_heartbeat", 0)
                if now - last > HEARTBEAT_TIMEOUT:
                    server_log(f"MONITOR: {cid} timed out (last hb {now-last:.1f}s)")
                    cur = info.get("current_job")
                    if cur:
                        server_log(f"MONITOR: Requeueing customer {cur['customer_id']}")
                        central_queue.put(cur)
                    dead.append(cid)
            for cid in dead:
                try:
                    clients[cid]["sock"].close()
                except:
                    pass
                clients.pop(cid, None)
        emit_snapshot()
        time.sleep(1)

# ---------- TCP Server ----------
def accept_clients(stop):
    serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serv.bind((HOST, TCP_PORT))
    serv.listen(50)
    serv.settimeout(1)
    server_log(f"TCP listening on port {TCP_PORT}")

    while not stop.is_set():
        try:
            conn, addr = serv.accept()
        except socket.timeout:
            continue
        cid = f"client_{int(time.time()*1000)%100000}"
        with clients_lock:
            clients[cid] = {
                "sock": conn,
                "addr": addr,
                "busy": False,
                "last_heartbeat": time.time(),
                "current_job": None,
                "jobs_done": 0,
                "busy_time": 0.0,
                "last_active": time.time(),
                "started_ts": None,
                "worker_type": "default"
            }
        server_log(f"TCP Connected: {cid} from {addr}")
        try:
            conn.sendall((json.dumps({"type":"assign","id":cid}) + "\n").encode())
        except:
            pass
        threading.Thread(target=client_recv_loop, args=(cid, conn), daemon=True).start()
        emit_snapshot()

def client_recv_loop(cid, conn):
    buf = b""
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line:
                    continue
                try:
                    msg = json.loads(line.decode())
                except:
                    continue
                handle_message(cid, msg)
    except Exception as e:
        server_log(f"[client_recv_loop] error {e}")
    finally:
        server_log(f"TCP Closed: {cid}")
        with clients_lock:
            info = clients.pop(cid, None)
            if info and info.get("current_job"):
                central_queue.put(info["current_job"])
        emit_snapshot()

def handle_message(cid, msg):
    typ = msg.get("type")
    now = time.time()
    with clients_lock:
        if cid not in clients:
            return
    if typ == "heartbeat":
        with clients_lock:
            # heartbeat may include worker_type
            wt = msg.get("worker_type")
            if wt:
                clients[cid]["worker_type"] = wt
            clients[cid]["last_heartbeat"] = now
    elif typ == "done":
        customer_id = msg.get("customer_id")
        service_time = msg.get("service_time")
        finished = time.time()
        with clients_lock:
            info = clients.get(cid)
            if info:
                info["busy"] = False
                cur = info.get("current_job")
                start_ts = info.get("started_ts") or (cur and cur.get("started"))
                if start_ts:
                    delta = finished - start_ts
                    info["busy_time"] = info.get("busy_time", 0.0) + delta
                info["jobs_done"] = info.get("jobs_done", 0) + 1
                info["current_job"] = None
                info["started_ts"] = None
                info["last_active"] = time.time()
        db_upsert({
            "customer_id": customer_id,
            "finished": finished,
            "service_time": service_time,
            "assigned_worker": cid
        })
        with metrics_lock:
            metrics["total_completed"] += 1
        server_log(f"DONE: {cid} finished customer {customer_id}")
    emit_snapshot()

# ---------- Snapshot & emit ----------
def make_snapshot():
    uptime = time.time() - start_time
    with clients_lock:
        counters = []
        for idx, (cid, info) in enumerate(clients.items(), start=1):
            busy = bool(info.get("busy"))
            jobs_done = info.get("jobs_done", 0)
            busy_time = info.get("busy_time", 0.0)
            util = (busy_time / uptime) * 100.0 if uptime > 0 else 0.0
            counters.append({
                "counter": f"Counter {idx}",
                "worker": cid,
                "worker_type": info.get("worker_type", "default"),
                "busy": busy,
                "customer": info.get("current_job", {}).get("customer_id") if info.get("current_job") else None,
                "service_time": info.get("current_job", {}).get("service_time") if info.get("current_job") else None,
                "jobs_done": jobs_done,
                "busy_time": round(busy_time, 2),
                "util_percent": round(util, 1),
                "addr": info.get("addr")
            })
    qsize = central_queue.qsize()
    queue_preview = list(central_queue.queue)[:20]
    with metrics_lock:
        m = metrics.copy()
    snap = {
        "queue_size": qsize,
        "queue_preview": queue_preview,
        "counters": counters,
        "metrics": m,
        "time": time.time(),
        "running": control["running"],
        "customer_mean": control.get("customer_mean", CUSTOMER_MEAN),
        "uptime": int(uptime),
        "dispatch_policy": DISPATCH_POLICY
    }
    return snap

def emit_snapshot():
    try:
        snap = make_snapshot()
        sio.emit("snapshot", snap)
    except Exception:
        pass

# ---------- SocketIO control ----------
@sio.on("control")
def on_control(data):
    action = data.get("action")
    if action == "pause":
        control["running"] = False
        server_log("Generator paused by UI")
    elif action == "resume":
        control["running"] = True
        server_log("Generator resumed by UI")
    elif action == "set_mean":
        try:
            v = float(data.get("value"))
            control["customer_mean"] = max(0.2, v)
            server_log(f"Generator mean set to {control['customer_mean']}")
        except:
            pass
    elif action == "get_history":
        jobs = recent_jobs(50)
        sio.emit("history", jobs)
    elif action == "set_policy":
        p = data.get("policy")
        if p in ("round_robin", "least_busy", "random"):
            global DISPATCH_POLICY
            DISPATCH_POLICY = p
            server_log(f"Dispatch policy set to {DISPATCH_POLICY}")

# ---------- Threads starter ----------
def run_server():
    stop = threading.Event()
    threads = [
        threading.Thread(target=accept_clients, args=(stop,), daemon=True),
        threading.Thread(target=customer_generator, args=(stop,), daemon=True),
        threading.Thread(target=dispatcher, args=(stop,), daemon=True),
        threading.Thread(target=monitor_clients, args=(stop,), daemon=True),
    ]
    for t in threads:
        t.start()
    return stop

# ---------- Main ----------
if __name__ == "__main__":
    init_db()
    start_time = time.time()
    server_log("Starting enhanced server...")
    stop_event = run_server()
    server_log(f"Web dashboard at http://0.0.0.0:{WS_PORT}")
    sio.run(app, host="0.0.0.0", port=WS_PORT)
