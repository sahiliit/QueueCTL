# queuectl – CLI Job Queue System

A lightweight, production‑minded CLI tool for managing background job execution using worker processes, retries with exponential backoff, and a Dead Letter Queue (DLQ). Built using Python + SQLite.

---

## ✅ Features

* Enqueue jobs with shell commands
* Multiple parallel worker processes
* Automatic retries with exponential backoff
* Persistent job history in SQLite (survives restarts)
* Dead Letter Queue for permanently failed jobs
* Graceful shutdown of workers
* Configuration management: retry limits, backoff base
* Single‑file implementation for simplicity

---

## 📍 used language

* Python 3.8 or higher
* SQLite included with Python ✅
---

## 🛠️ Setup Instructions

1️⃣ Clone or download the `queuectl.py` file into any folder.

Example on Windows:

```sh
C:\queuectl
└── queuectl.py
```

2️⃣ Open **PowerShell** or **VS Code Terminal** inside that folder and test:

```powershell
python queuectl.py selftest
```

If jobs are processed ✅ setup is complete!

---

## 🚀 Usage Examples

> Use separate terminals: one for workers, one for commands

### ▶ Start Workers

```powershell
python queuectl.py worker start --count 2
```

Workers will continuously fetch and process jobs.

### ➕ Enqueue a Job

```powershell
python queuectl.py enqueue '{"id":"job1","command":"echo Hello"}'
```

### 📊 Status Summary

```powershell
python queuectl.py status
```

Expected Output Example:

```
Workers active: 2
Jobs by state:
  pending: 0
  completed: 1
  dead: 1
```

### 📋 List Jobs by State

```powershell
python queuectl.py list --state completed
```

### ⚠️ Dead Letter Queue

List DLQ jobs:

```powershell
python queuectl.py dlq list
```

Retry a failed job:

```powershell
python queuectl.py dlq retry job1
```

### 🛑 Stop Workers

```powershell
python queuectl.py worker stop
```

---

## 🔁 Retry & Backoff Logic

Failed jobs retry automatically using:

```
delay = backoff_base ^ attempts
```

Example (backoff_base=2):

| Attempts | Delay     |
| -------- | --------- |
| 1        | 2 seconds |
| 2        | 4 seconds |
| 3        | 8 seconds |

If retries exceed `max_retries` → job becomes `dead` ✅

---

## 🧱 Architecture Overview

db: `~/.queuectl/jobs.db`

### 📌 Job Lifecycle

| State      | Meaning                             |
| ---------- | ----------------------------------- |
| pending    | Awaiting execution                  |
| processing | A worker is running it              |
| completed  | Finished successfully               |
| failed     | Temporary failure → retry scheduled |
| dead       | Exhausted retries → moved to DLQ    |

### ⚙ Worker Processing

1. Atomically claim next pending job
2. Execute shell command
3. Update job state based on exit code
4. Schedule retry or move to DLQ

Uses SQLite row‑locking to prevent duplicate execution ✅

---

## 📝 Assumptions & Trade‑offs

✅ Simplicity valued over distributed scalability
✅ Local file DB instead of Redis/RabbitMQ/Kafka
✅ Commands executed via shell for flexibility
⚠ Workers must stay alive to process backoff retries
⚠ No push notifications / web dashboard

---

## 🧪 Testing Instructions

Run full self‑test:

```powershell
python queuectl.py selftest
```

Manual validation checklist:

1. Job completes successfully ✅
2. Invalid command → retries → DLQ ✅
3. Multiple workers don’t duplicate processing ✅
4. `queuectl.py worker stop` ends gracefully ✅
5. Restart process → jobs remain ✅

---

## ✅ Status

✔ Implemented
🔜 Future Enhancements:

* Logging to file
* Web dashboard
* Job scheduling (cron‑style)

---

Maintained by: **Sahil’s Queue System** 🚀
