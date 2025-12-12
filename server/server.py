"""
server.py - Banking Multiprogramming Server (enhanced)
Includes Amdahl's law representation and measured speedup metrics.
Run: python server.py
"""
import socket
import threading
import json
import time
import random
import queue
import sqlite3
from flask import Flask, send_from_directory
from flask_socketio import SocketIO

# ---------------- CONFIG ----------------
HOST = ""
TCP_PORT = 9999
WS_PORT = 5000

HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 6.0

CUSTOMER_MEAN = 1.2   # adjustable from UI
SERVICE_MIN = 2.0
SERVICE_MAX = 5.0

DB_FILE = "server_data.db"

# ---------------- Flask + SocketIO ----------------
app = Flask(__name__, static_folder="static")
sio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

# ---------------- DB helper ----------------
_db_lock = threading.Lock()

def init_db():
    with _db_lock:
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

def db_upsert_raw(row):
    """Insert or replace using a dict row; key names as used in code."""
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO customers
            (id, arrival, dispatched, started, finished, service_time, assigned_worker)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            row.get("customer_id"),
            row.get("arrival"),
            row.get("dispatched"),
            row.get("started"),
            row.get("finished"),
            row.get("service_time"),
            row.get("assigned_worker"),
        ))
        conn.commit()
        conn.close()

def db_get_job(customer_id):
    """Return full job dict for the given customer_id or None."""
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT id, arrival, dispatched, started, finished, service_time, assigned_worker FROM customers WHERE id = ?", (customer_id,))
        r = c.fetchone()
        conn.close()
    if not r:
        return None
    return {
        "customer_id": r[0],
        "arrival": r[1],
        "dispatched": r[2],
        "started": r[3],
        "finished": r[4],
        "service_time": r[5],
        "assigned_worker": r[6]
    }

def recent_jobs(limit=50):
    with _db_lock:
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

# ---------------- Shared state ----------------
central_queue = queue.Queue()
clients = {}   # client_id -> { sock, addr, busy, last_heartbeat, current_job, jobs_done, busy_time, last_active, started_ts }
clients_lock = threading.Lock()

metrics = {
    "total_generated": 0,
    "total_dispatched": 0,
    "total_completed": 0,
    "total_service_time": 0.0
}
metrics_lock = threading.Lock()

# control flags
control = {
    "running": True,
    "customer_mean": CUSTOMER_MEAN,
    "serial_fraction": 0.2
}

# ---------------- logging helper (also emits to UI) ----------------
def server_log(msg):
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        sio.emit("log", line)
    except Exception:
        pass

# ---------------- Customer generator ----------------
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
        db_upsert_raw(cust)
        with metrics_lock:
            metrics["total_generated"] += 1
        server_log(f"New customer {cid} st={st}")
        emit_snapshot()

# ---------------- Dispatcher ----------------
# In server.py, inside the dispatcher function
# BEFORE assigning the task to a worker:

def dispatcher(stop):
    while not stop.is_set():
        try:
            cust = central_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        # --- FIX START: APPLY AMDAHL'S PENALTY ---
        # If serial_fraction is 0.2 (20%), and task takes 2.0s:
        # We assume the TOTAL time is split between Serial (Server) and Parallel (Worker).
        # We must artificially delay the Dispatcher (Serial bottleneck) to simulate this.
        
        fraction = control.get("serial_fraction", 0.0)
        if fraction > 0:
            # We delay the server from dispatching the NEXT job
            # proportional to the job's size
            delay_time = cust["service_time"] * fraction
            time.sleep(delay_time) 
        # --- FIX END ---

        assigned = False
        with clients_lock:
            for cid, info in clients.items():
                if not info.get("busy"):
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
                        db_upsert_raw(cust)

                        # Emit full job record (read from DB to guarantee consistent fields)
                        try:
                            job_full = db_get_job(cust["customer_id"])
                            if job_full:
                                sio.emit("history_update", job_full)
                        except Exception:
                            pass

                        info["jobs_done"] = info.get("jobs_done", 0)

                        with metrics_lock:
                            metrics["total_dispatched"] += 1

                        server_log(f"DISPATCH: Customer {cust['customer_id']} -> {cid}")
                        assigned = True
                        break
                    except Exception as e:
                        server_log(f"Dispatch send error to {cid}: {e}")
                        continue

        if not assigned:
            central_queue.put(cust)

        emit_snapshot()

# ---------------- Heartbeat monitor ----------------
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

# ---------------- TCP Server ----------------
def accept_clients(stop):
    serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serv.bind((HOST, TCP_PORT))
    serv.listen(20)
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
                "started_ts": None
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
            clients[cid]["last_heartbeat"] = now

    elif typ == "done":
        customer_id = msg.get("customer_id")
        service_time = msg.get("service_time")
        try:
            service_time = float(service_time)
        except:
            service_time = None
        finished = time.time()

        with clients_lock:
            info = clients.get(cid)
            if info:
                info["busy"] = False
                cur = info.get("current_job")
                start_ts = info.get("started_ts") or (cur.get("started") if cur else None)
                if start_ts:
                    delta = finished - start_ts
                    info["busy_time"] = info.get("busy_time", 0.0) + delta
                info["jobs_done"] = info.get("jobs_done", 0) + 1
                info["current_job"] = None
                info["started_ts"] = None
                info["last_active"] = time.time()

        # upsert finished details
        db_upsert_raw({
            "customer_id": customer_id,
            "finished": finished,
            "service_time": service_time,
            "assigned_worker": cid
        })

        # read full row and emit to UI (guarantees arrival/dispatched/etc are present if DB had them)
        try:
            job_full = db_get_job(customer_id)
            if job_full:
                sio.emit("history_update", job_full)
        except Exception:
            pass

        with metrics_lock:
            metrics["total_completed"] += 1
            if service_time:
                metrics["total_service_time"] += float(service_time)

        server_log(f"DONE: {cid} finished customer {customer_id}")
    emit_snapshot()

# ---------------- Snapshot, Amdahl & control ----------------
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

    N = max(1, len(clients))
    serial = float(control.get("serial_fraction", 0.2))
    theoretical = 1.0 / (serial + (1.0 - serial) / float(N))
    total_completed = m.get("total_completed", 0)
    total_service_time = m.get("total_service_time", 0.0)
    avg_service = (total_service_time / total_completed) if total_completed > 0 else ((SERVICE_MIN + SERVICE_MAX) / 2.0)
    single_worker_throughput = (1.0 / avg_service) if avg_service > 0 else 0.0
    current_throughput = (total_completed / uptime) if uptime > 0 else 0.0
    measured_speedup = (current_throughput / single_worker_throughput) if single_worker_throughput > 0 else 0.0

    snap = {
        "queue_size": qsize,
        "queue_preview": queue_preview,
        "counters": counters,
        "metrics": m,
        "time": time.time(),
        "running": control["running"],
        "customer_mean": control.get("customer_mean", CUSTOMER_MEAN),
        "serial_fraction": serial,
        "theoretical_speedup": round(theoretical, 3),
        "measured_speedup": round(measured_speedup, 3),
        "single_worker_throughput": round(single_worker_throughput, 3),
        "current_throughput": round(current_throughput, 3),
        "worker_count": N,
        "uptime": int(uptime)
    }
    return snap

def emit_snapshot():
    try:
        snap = make_snapshot()
        sio.emit("snapshot", snap)
    except Exception:
        pass

# expose control events from UI
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
    elif action == "set_serial":
        try:
            v = float(data.get("value"))
            control["serial_fraction"] = min(1.0, max(0.0, v))
            server_log(f"Serial fraction set to {control['serial_fraction']}")
        except:
            pass
    elif action == "get_history":
        jobs = recent_jobs(50)
        sio.emit("history", jobs)

# ---------------- Threads starter ----------------
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

# ---------------- Main ----------------
if __name__ == "__main__":
    init_db()
    start_time = time.time()
    server_log("Starting server (with Amdahl metrics + real-time history)...")
    stop_event = run_server()
    server_log(f"Web dashboard at http://0.0.0.0:{WS_PORT}")
    sio.run(app, host="0.0.0.0", port=WS_PORT)