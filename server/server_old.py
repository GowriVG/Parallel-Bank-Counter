"""
server.py
- TCP server (port 9999) that accepts worker clients
- Heartbeat-based fault detection and automatic reassignment
- Central job generator (simulates arriving customers)
- Dispatcher: assigns jobs to connected clients or local pool
- Local CPU-bound processing via multiprocessing.Pool
- Web dashboard using Flask + Flask-SocketIO (threading)
Notes:
- Multiprocessing Pool is created inside main to be Windows-safe.
- Run: python server.py
"""
import socket
import threading
import json
import time
import random
import queue
import sqlite3
from multiprocessing import Pool
from flask import Flask, send_from_directory
from flask_socketio import SocketIO

# ---------------- CONFIG ----------------
HOST = ""             # bind all interfaces
TCP_PORT = 9999       # port for TCP clients (workers)
WS_PORT = 5000        # dashboard port
HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 6.0

CUSTOMER_MEAN = 1.2   # avg inter-arrival seconds
SERVICE_MIN = 2.0
SERVICE_MAX = 5.0

LOCAL_CPU_WORKERS = 2  # number of local pool processes

DB_FILE = "server_data.db"

# ---------------- DB helper (simple sqlite logging) ----------------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS customers (
                 id INTEGER PRIMARY KEY,
                 arrival REAL, dispatched REAL, started REAL, finished REAL,
                 service_time REAL, assigned_worker TEXT)""")
    conn.commit()
    conn.close()

def db_upsert(cust):
    # cust: dict with keys customer_id, arrival, dispatched, started, finished, service_time, assigned_worker
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""INSERT OR REPLACE INTO customers
                 (id, arrival, dispatched, started, finished, service_time, assigned_worker)
                 VALUES (?, ?, ?, ?, ?, ?, ?)""",
              (cust.get("customer_id"),
               cust.get("arrival"),
               cust.get("dispatched"),
               cust.get("started"),
               cust.get("finished"),
               cust.get("service_time"),
               cust.get("assigned_worker")))
    conn.commit()
    conn.close()

# ---------------- Flask + SocketIO ----------------
app = Flask(__name__, static_folder="static")
sio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

# ---------------- Shared state (in-process, thread-safe with locks) ----------------
central_queue = queue.Queue()
clients = {}            # client_id -> {sock, addr, busy(bool), last_heartbeat, current_job}
clients_lock = threading.Lock()

metrics = {
    "total_generated": 0,
    "total_dispatched": 0,
    "total_completed": 0
}
metrics_lock = threading.Lock()

# pool will be created in main()
local_pool = None

# ---------------- Customer generator ----------------
cust_seq = 1
def customer_generator(stop_event):
    global cust_seq
    while not stop_event.is_set():
        delay = max(0.2, random.expovariate(1.0 / CUSTOMER_MEAN))
        time.sleep(delay)
        cid = cust_seq
        cust_seq += 1
        st = round(random.uniform(SERVICE_MIN, SERVICE_MAX), 2)
        cust = {
            "customer_id": cid,
            "arrival": time.time(),
            "service_time": st
        }
        central_queue.put(cust)
        db_upsert({**cust})
        with metrics_lock:
            metrics["total_generated"] += 1
        print(f"[GENERATOR] New customer {cid} st={st}")
        emit_snapshot()

# ---------------- CPU-bound task (runs in worker process) ----------------
def cpu_heavy_task(cust):
    # Simulate CPU work proportional to service_time
    st = max(1.0, cust.get("service_time", 2.0))
    iterations = int(150000 * st)
    acc = 0
    for i in range(iterations):
        acc += (i ^ (i << 1)) & 0xFFFF
    # record started/finished times (in main process we will update started/finished)
    return cust["customer_id"]

# callback runs in main thread after pool completes
def local_done_callback(cust_id):
    # update metrics and snapshot
    with metrics_lock:
        metrics["total_completed"] += 1
    print(f"[LOCAL] Completed customer {cust_id}")
    emit_snapshot()

# ---------------- Dispatcher: assign to free clients or local pool ----------------
def dispatcher(stop_event):
    while not stop_event.is_set():
        try:
            cust = central_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        assigned = False
        with clients_lock:
            # try find a free client
            for cid, info in list(clients.items()):
                if not info.get("busy"):
                    try:
                        sock = info["sock"]
                        msg = {"type":"job","customer_id":cust["customer_id"], "service_time":cust["service_time"]}
                        sock.sendall((json.dumps(msg) + "\n").encode())
                        info["busy"] = True
                        info["current_job"] = cust
                        info["last_heartbeat"] = time.time()
                        clients[cid] = info
                        assigned = True
                        cust["dispatched"] = time.time()
                        cust["assigned_worker"] = cid
                        db_upsert(cust)
                        with metrics_lock:
                            metrics["total_dispatched"] += 1
                        print(f"[DISPATCH] Sent customer {cust['customer_id']} -> {cid}")
                        break
                    except Exception as e:
                        print("[DISPATCH] send error to", cid, e)
                        # mark client dead; will be cleaned by monitor
                        continue

        if not assigned:
            # send to local pool
            cust["dispatched"] = time.time()
            cust["assigned_worker"] = "local_pool"
            db_upsert(cust)
            with metrics_lock:
                metrics["total_dispatched"] += 1
            # record start time in main, then submit
            cust["started"] = time.time()
            if local_pool:
                local_pool.apply_async(cpu_heavy_task, args=(cust,), callback=lambda cid: local_done_callback(cid))
                print(f"[DISPATCH] Sent customer {cust['customer_id']} -> local_pool")
            else:
                # fallback: do in a background thread sleep
                def fallback(c):
                    time.sleep(c.get("service_time", 2))
                    local_done_callback(c["customer_id"])
                threading.Thread(target=fallback, args=(cust,), daemon=True).start()

        emit_snapshot()

# ---------------- Monitor clients for heartbeat timeouts ----------------
def monitor_clients(stop_event):
    while not stop_event.is_set():
        now = time.time()
        dead = []
        with clients_lock:
            for cid, info in list(clients.items()):
                last = info.get("last_heartbeat", 0)
                if now - last > HEARTBEAT_TIMEOUT:
                    print(f"[MONITOR] Client {cid} timed out (last hb {now-last:.1f}s).")
                    cur = info.get("current_job")
                    if cur:
                        print(f"[MONITOR] Reassigning customer {cur['customer_id']} back to central queue")
                        central_queue.put(cur)
                    dead.append(cid)
            for cid in dead:
                try:
                    sock = clients[cid]["sock"]
                    try:
                        sock.close()
                    except:
                        pass
                except:
                    pass
                clients.pop(cid, None)
        emit_snapshot()
        time.sleep(1.5)

# ---------------- TCP accept and recv loops ----------------
def accept_clients(stop_event):
    serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serv.bind((HOST, TCP_PORT))
    serv.listen(10)
    serv.settimeout(1.0)
    print(f"[TCP] Listening on port {TCP_PORT}")
    while not stop_event.is_set():
        try:
            conn, addr = serv.accept()
        except socket.timeout:
            continue
        cid = f"client_{int(time.time()*1000)%100000}"
        with clients_lock:
            clients[cid] = {"sock":conn, "addr":addr, "busy":False, "last_heartbeat":time.time(), "current_job":None}
        print(f"[TCP] Connected {cid} from {addr}")
        try:
            conn.sendall((json.dumps({"type":"assign","id":cid}) + "\n").encode())
        except:
            pass
        threading.Thread(target=client_recv_loop, args=(cid,conn), daemon=True).start()
        emit_snapshot()

def client_recv_loop(cid, conn):
    buf = b''
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buf += data
            while b'\n' in buf:
                line, buf = buf.split(b'\n', 1)
                if not line:
                    continue
                try:
                    msg = json.loads(line.decode())
                except Exception:
                    continue
                handle_client_message(cid, msg)
    except Exception as e:
        print("[client_recv_loop] error", e)
    finally:
        print(f"[TCP] connection closed {cid}")
        with clients_lock:
            info = clients.pop(cid, None)
            if info and info.get("current_job"):
                central_queue.put(info["current_job"])
        emit_snapshot()

def handle_client_message(cid, msg):
    typ = msg.get("type")
    if typ == "heartbeat":
        with clients_lock:
            if cid in clients:
                clients[cid]["last_heartbeat"] = time.time()
    elif typ == "done":
        customer_id = msg.get("customer_id")
        service_time = msg.get("service_time")
        finished = time.time()
        record = {"customer_id": customer_id, "finished": finished, "service_time": service_time, "assigned_worker": cid}
        db_upsert(record)
        with clients_lock:
            if cid in clients:
                clients[cid]["busy"] = False
                clients[cid]["current_job"] = None
        with metrics_lock:
            metrics["total_completed"] += 1
        print(f"[DONE] Client {cid} finished customer {customer_id}")
        emit_snapshot()
    else:
        # unknown message types may be supported later
        pass

# ---------------- Snapshot for dashboard ----------------
def make_snapshot():
    with clients_lock:
        clients_info = {cid: {"busy": info.get("busy"), "last_heartbeat": info.get("last_heartbeat"), "addr": info.get("addr")} for cid, info in clients.items()}
    qsize = central_queue.qsize()
    with metrics_lock:
        m = metrics.copy()
    snap = {"clients": clients_info, "queue_size": qsize, "metrics": m, "time": time.time()}
    return snap

def emit_snapshot():
    try:
        snap = make_snapshot()
        sio.emit("snapshot", snap)
    except Exception:
        pass

# ---------------- Web emitter thread (periodic) ----------------
def snapshot_emitter(stop_event):
    while not stop_event.is_set():
        emit_snapshot()
        time.sleep(1.0)

# ---------------- MAIN: create pool and start threads (Windows-safe) ----------------
def run_server():
    stop = threading.Event()
    # threads
    threads = []
    threads.append(threading.Thread(target=accept_clients, args=(stop,), daemon=True))
    threads.append(threading.Thread(target=customer_generator, args=(stop,), daemon=True))
    threads.append(threading.Thread(target=dispatcher, args=(stop,), daemon=True))
    threads.append(threading.Thread(target=monitor_clients, args=(stop,), daemon=True))
    threads.append(threading.Thread(target=snapshot_emitter, args=(stop,), daemon=True))

    for t in threads:
        t.start()
    return stop, threads

if __name__ == "__main__":
    print("[MAIN] Initializing DB...")
    init_db()
    # create local Pool here (inside main) - Windows safe
    print(f"[MAIN] Creating local process pool with {LOCAL_CPU_WORKERS} workers")
    local_pool = Pool(processes=LOCAL_CPU_WORKERS)

    # start server threads
    stop_event, ths = run_server()

    # run flask socketio (this blocks; snapshot_emitter thread sends updates)
    print(f"[MAIN] Web dashboard at http://0.0.0.0:{WS_PORT}")
    sio.run(app, host="0.0.0.0", port=WS_PORT)
    # when socketio stops, set stop_event to shut threads (rare in development)
    stop_event.set()
    local_pool.close()
    local_pool.join()
