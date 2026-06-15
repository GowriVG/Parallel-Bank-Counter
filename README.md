# Parallel Bank Counter

A distributed simulation of a parallel bank queuing system that demonstrates **multiprogramming**, **Amdahl's Law**, and **real-time parallel speedup metrics**. Customers are generated on the server, dispatched to multiple worker clients over TCP, and visualised through a live web dashboard.

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│                    SERVER                        │
│                                                  │
│  Customer Generator ──► Central Queue            │
│                              │                   │
│                         Dispatcher               │
│                         (+ Amdahl serial delay)  │
│                              │                   │
│           ┌──────────────────┼──────────────────┐│
│        Worker 1           Worker 2         ...  ││
│        (client.py)        (client.py)           ││
└──────────────────────────────────────────────────┘
              TCP :9999          WebSocket :5000
                                      │
                              Browser Dashboard
                              (index.html)
```

- **Server** manages the queue, dispatches jobs, monitors heartbeats, and stores results in SQLite.
- **Clients** connect over TCP, receive jobs, simulate processing (CPU or sleep), and report back.
- **Dashboard** streams live metrics — worker status, queue depth, speedup, and job history — via Socket.IO.

---

## Features

- Poisson customer arrival with configurable mean inter-arrival time
- Random service times drawn from a uniform distribution
- Dynamic worker pool — connect or disconnect clients at any time
- Heartbeat-based fault detection with automatic job re-queuing on worker failure
- **Amdahl's Law** serial fraction simulation (adjustable from the UI)
- Real-time speedup and efficiency metrics
- SQLite persistence for all job lifecycle timestamps
- Dark-mode web dashboard (no build step required)

---

## Requirements

### Server
- Python 3.8+
- Flask
- Flask-SocketIO
- eventlet
- python-socketio
- psutil

### Client
- Python 3.8+
- psutil

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/Parallel-Bank-Counter.git
cd Parallel-Bank-Counter
```

### 2. Install server dependencies

```bash
cd server
pip install -r requirements.txt
```

### 3. Install client dependencies

```bash
cd ../client
pip install -r requirements.txt
```

---

## Running

### Start the server

```bash
cd server
python server.py
```

The server starts two listeners:
| Service | Address |
|---------|---------|
| TCP job dispatcher | `0.0.0.0:9999` |
| Web dashboard | `http://localhost:5000` |

### Open the dashboard

Navigate to [http://localhost:5000](http://localhost:5000) in your browser.

### Connect one or more worker clients

Run the following on the **same machine** or any machine on the same network:

```bash
cd client
python client.py <server_ip>
```

To simulate CPU-bound work instead of sleeping:

```bash
python client.py <server_ip> --simulate-cpu
```

You can open multiple terminals and run several clients simultaneously to observe parallel speedup.

---

## Configuration

Key constants at the top of `server/server.py`:

| Constant | Default | Description |
|----------|---------|-------------|
| `TCP_PORT` | `9999` | Port for worker client connections |
| `WS_PORT` | `5000` | Port for the web dashboard |
| `CUSTOMER_MEAN` | `1.2` s | Mean inter-arrival time (Poisson) |
| `SERVICE_MIN` | `2.0` s | Minimum service time per job |
| `SERVICE_MAX` | `5.0` s | Maximum service time per job |
| `HEARTBEAT_INTERVAL` | `2.0` s | How often clients send a heartbeat |
| `HEARTBEAT_TIMEOUT` | `6.0` s | Time before a silent client is declared dead |

The **serial fraction** (Amdahl's Law) and **customer mean** can also be adjusted live from the dashboard UI.

---

## Project Structure

```
Parallel-Bank-Counter/
├── server/
│   ├── server.py          # Main server: queue, dispatcher, metrics, Flask app
│   ├── requirements.txt
│   └── static/
│       └── index.html     # Real-time web dashboard
├── client/
│   ├── client.py          # Worker client
│   └── requirements.txt
└── README.md
```

---

## How It Works

1. The **customer generator** thread produces customers at random intervals following a Poisson process and pushes them onto a central queue.
2. The **dispatcher** thread pulls customers from the queue and assigns each one to the first available (idle) worker client over TCP.
3. To model **Amdahl's Law**, the dispatcher optionally sleeps for `service_time × serial_fraction` before sending the next job, simulating a sequential bottleneck.
4. Each **worker client** receives a job, processes it (sleep or CPU loop), then sends a `done` message back.
5. The server updates the SQLite database with full lifecycle timestamps (`arrival → dispatched → started → finished`).
6. All state changes are broadcast to the browser dashboard in real time via **Socket.IO**.

---

## License

MIT
