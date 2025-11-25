import socket
import threading
import json
import time
import sys

PORT = 9999

class Client:
    def __init__(self, server_ip):
        self.server_ip = server_ip
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.running = True
        self.busy = False

    def start(self):
        print(f"[CLIENT] Connecting to {self.server_ip}:{PORT}")
        self.sock.connect((self.server_ip, PORT))
        print("[CLIENT] Connected!")

        threading.Thread(target=self.listen, daemon=True).start()

        while self.running:
            time.sleep(1)

    def listen(self):
        buf = b""
        while self.running:
            try:
                data = self.sock.recv(4096)
                if not data:
                    break

                buf += data

                if b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    msg = json.loads(line.decode())

                    if msg["type"] == "assign_id":
                        print(f"[CLIENT] ID assigned: {msg['client_id']}")

                    elif msg["type"] == "job":
                        cust = msg["customer_id"]
                        st = msg["service_time"]

                        print(f"[CLIENT] Serving customer {cust} ({st}s)")

                        self.busy = True
                        time.sleep(st)
                        self.busy = False

                        self.sock.sendall(json.dumps({
                            "type": "done",
                            "customer_id": cust,
                            "service_time": st
                        }).encode() + b"\n")

            except:
                break

        self.running = False
        print("[CLIENT] Disconnected")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python client.py <server_ip>")
        sys.exit(1)

    server_ip = sys.argv[1]

    c = Client(server_ip)
    c.start()
