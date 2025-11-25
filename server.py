import socket
import threading
import json
import time
import random
import queue
import tkinter as tk
from tkinter import ttk

HOST = ''
PORT = 9999

CUSTOMER_ARRIVAL_MEAN = 1.5
SERVICE_TIME_MIN = 2.0
SERVICE_TIME_MAX = 5.0

GUI_UPDATE_MS = 500


class ClientInfo:
    def __init__(self, conn, addr, client_id):
        self.conn = conn
        self.addr = addr
        self.id = client_id
        self.busy = False
        self.current_customer = None
        self.last_seen = time.time()


class Server:
    def __init__(self, local_counters=2):
        self.local_counters = local_counters
        self.local_counter_queues = [queue.Queue() for _ in range(local_counters)]
        self.local_counter_status = [False] * local_counters

        self.customer_queue = queue.Queue()

        self.clients = {}
        self.clients_lock = threading.Lock()

        self.total_served = 0
        self.customer_id_seq = 1
        self.start_time = time.time()

        self.running = True
        self.distribute_to_clients = True

    def start_network(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((HOST, PORT))
        self.sock.listen(5)
        print(f"[SERVER] Listening on port {PORT}")

        threading.Thread(target=self.accept_clients, daemon=True).start()
        threading.Thread(target=self.generate_customers, daemon=True).start()
        threading.Thread(target=self.dispatcher, daemon=True).start()

        for i in range(self.local_counters):
            threading.Thread(target=self.local_counter, args=(i,), daemon=True).start()

    def accept_clients(self):
        cid = 1
        while self.running:
            conn, addr = self.sock.accept()
            client_id = f"client{cid}"
            cid += 1

            ci = ClientInfo(conn, addr, client_id)

            with self.clients_lock:
                self.clients[client_id] = ci

            threading.Thread(target=self.client_handler, args=(ci,), daemon=True).start()
            print(f"[SERVER] Connected: {client_id}")

    def client_handler(self, ci):
        conn = ci.conn
        self.send(conn, {"type": "assign_id", "client_id": ci.id})

        try:
            while self.running:
                data = conn.recv(4096)
                if not data:
                    break

                msgs = data.decode().strip().split("\n")
                for line in msgs:
                    if not line:
                        continue
                    msg = json.loads(line)

                    if msg["type"] == "done":
                        ci.busy = False
                        ci.current_customer = None
                        self.total_served += 1

        except:
            pass

        finally:
            with self.clients_lock:
                if ci.id in self.clients:
                    del self.clients[ci.id]
            print(f"[SERVER] Disconnected: {ci.id}")

    def send(self, conn, data):
        try:
            conn.sendall((json.dumps(data) + "\n").encode())
        except:
            pass

    def generate_customers(self):
        while self.running:
            time.sleep(random.expovariate(1.0 / CUSTOMER_ARRIVAL_MEAN))

            cid = self.customer_id_seq
            self.customer_id_seq += 1

            st = round(random.uniform(SERVICE_TIME_MIN, SERVICE_TIME_MAX), 2)
            customer = {"customer_id": cid, "service_time": st}

            self.customer_queue.put(customer)
            print(f"[GENERATOR] New customer {cid}")

    def dispatcher(self):
        while self.running:
            try:
                cust = self.customer_queue.get(timeout=0.5)
            except:
                continue

            # Try assigning to free client
            assigned = False

            if self.distribute_to_clients:
                with self.clients_lock:
                    free = [c for c in self.clients.values() if not c.busy]

                if free:
                    target = free[0]
                    self.send(target.conn, {
                        "type": "job",
                        "customer_id": cust["customer_id"],
                        "service_time": cust["service_time"]
                    })
                    target.busy = True
                    target.current_customer = cust["customer_id"]
                    assigned = True
                    continue

            # Assign to local counter
            if not assigned:
                idx = min(range(self.local_counters),
                          key=lambda x: self.local_counter_queues[x].qsize())

                self.local_counter_queues[idx].put(cust)

    def local_counter(self, idx):
        q = self.local_counter_queues[idx]
        while self.running:
            try:
                cust = q.get(timeout=0.5)
            except:
                continue

            self.local_counter_status[idx] = True
            print(f"[LOCAL COUNTER {idx}] Serving {cust['customer_id']}")

            time.sleep(cust["service_time"])
            self.total_served += 1

            self.local_counter_status[idx] = False

    def snapshot(self):
        with self.clients_lock:
            clients = [{"id": c.id, "busy": c.busy, "current": c.current_customer}
                       for c in self.clients.values()]

        return {
            "clients": clients,
            "local_status": list(self.local_counter_status),
            "local_queue_sizes": [q.qsize() for q in self.local_counter_queues],
            "central_queue": self.customer_queue.qsize(),
            "total_served": self.total_served,
            "uptime": int(time.time() - self.start_time)
        }


class ServerGUI:
    def __init__(self, server):
        self.server = server
        self.root = tk.Tk()
        self.root.title("Parallel Bank Queue Simulator - Server")

        frame = ttk.Frame(self.root, padding=10)
        frame.pack(fill="both", expand=True)

        ttk.Label(frame, text="Connected Clients:").pack()

        self.tree = ttk.Treeview(frame, columns=("busy", "current"), show="headings")
        self.tree.heading("busy", text="Busy")
        self.tree.heading("current", text="Current Customer")
        self.tree.pack()

        self.local_labels = []
        for i in range(server.local_counters):
            lbl = ttk.Label(frame, text=f"Counter {i}: Free")
            lbl.pack()
            self.local_labels.append(lbl)

        self.stats = ttk.Label(frame, text="Served: 0 | Uptime: 0s")
        self.stats.pack(pady=10)

        tk.Button(frame, text="Toggle Network Dispatch",
                  command=self.toggle).pack()

        self.update_gui()
        self.root.mainloop()

    def toggle(self):
        self.server.distribute_to_clients = not self.server.distribute_to_clients

    def update_gui(self):
        snap = self.server.snapshot()

        # Update clients table
        for i in self.tree.get_children():
            self.tree.delete(i)

        for c in snap["clients"]:
            self.tree.insert("", "end", values=(c["busy"], c["current"]))

        # Update local counters
        for i, lbl in enumerate(self.local_labels):
            state = "Busy" if snap["local_status"][i] else "Free"
            q = snap["local_queue_sizes"][i]
            lbl.config(text=f"Counter {i}: {state} (queue={q})")

        self.stats.config(text=f"Served: {snap['total_served']} | Uptime: {snap['uptime']}s")

        self.root.after(GUI_UPDATE_MS, self.update_gui)


if __name__ == "__main__":
    server = Server(local_counters=2)
    server.start_network()

    gui = ServerGUI(server)
