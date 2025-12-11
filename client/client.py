"""
client.py
Usage: python client.py <server_ip> [--simulate-cpu]

Connects to server TCP port 9999, receives jobs, does CPU or sleep,
sends heartbeat and sends done messages.
"""
import socket
import sys
import threading
import time
import json

TCP_PORT = 9999
HEARTBEAT_INTERVAL = 2.0

def cpu_heavy(service_time):
    iterations = int(150000 * max(1.0, service_time))
    acc = 0
    for i in range(iterations):
        acc += (i ^ (i << 1)) & 0xFFFF
    return acc

def send_heartbeat(sock, client_id):
    while True:
        try:
            sock.sendall((json.dumps({"type":"heartbeat","id":client_id}) + "\n").encode())
        except Exception:
            break
        time.sleep(HEARTBEAT_INTERVAL)

def handle_server(sock, simulate_cpu):
    buf = b''
    client_id = None
    while True:
        data = sock.recv(4096)
        if not data:
            print("[CLIENT] server closed")
            break
        buf += data
        while b'\n' in buf:
            line, buf = buf.split(b'\n', 1)
            if not line: continue
            try:
                msg = json.loads(line.decode())
            except:
                continue
            typ = msg.get('type')
            if typ == 'assign':
                client_id = msg.get('id')
                print("[CLIENT] assigned id", client_id)
                threading.Thread(target=send_heartbeat, args=(sock,client_id), daemon=True).start()
            elif typ == 'job':
                cust = msg.get('customer_id')
                st = float(msg.get('service_time', 2.0))
                print(f"[CLIENT] Received job {cust} st={st}")
                if simulate_cpu:
                    cpu_heavy(st)
                else:
                    time.sleep(st)
                done = {"type":"done", "customer_id": cust, "service_time": st}
                try:
                    sock.sendall((json.dumps(done) + "\n").encode())
                except Exception as e:
                    print("[CLIENT] failed to send done", e)

def main():
    if len(sys.argv) < 2:
        print("Usage: python client.py <server_ip> [--simulate-cpu]")
        sys.exit(1)
    server_ip = sys.argv[1]
    simulate_cpu = ("--simulate-cpu" in sys.argv)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_ip, TCP_PORT))
    print("[CLIENT] Connected to server", server_ip)
    handle_server(sock, simulate_cpu)

if __name__ == "__main__":
    main()