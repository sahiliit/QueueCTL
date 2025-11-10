#!/usr/bin/env python3
"""
queuectl — Simple CLI background job queue with workers, retries, exponential backoff and DLQ
=========================================================================================

Single-file implementation intended for demonstration and lightweight usage.

Features
- SQLite persistent storage (jobs, config)
- CLI commands: enqueue, worker start/stop, status, list, dlq list, dlq retry, config set/get
- Multiple worker processes
- Atomic job claim using SQLite row update
- Exponential backoff: delay = base ** attempts (seconds)
- Dead Letter Queue after exhausting retries
- Graceful shutdown: master PID file and signal handling; workers finish current job

Usage
------
Make executable: chmod +x queuectl.py

Examples:
  ./queuectl.py enqueue '{"id":"job1","command":"sleep 2","max_retries":3}'
  ./queuectl.py worker start --count 3
  ./queuectl.py worker stop
  ./queuectl.py status
  ./queuectl.py list --state pending
  ./queuectl.py dlq list
  ./queuectl.py dlq retry job1
  ./queuectl.py config set max_retries 5

Design notes
------------
- This is intentionally minimal but production-minded: SQLite transactions are used to avoid duplicate job processing.
- Configuration is saved in the DB and can be changed at runtime.

Limitations / TODO
- No authentication, no remote operation.
- On Windows, signal behavior may differ.

"""

import argparse
import sqlite3
import os
import sys
import json
import time
import datetime
import subprocess
import uuid
import signal
import multiprocessing
from multiprocessing import Process, Event
from contextlib import contextmanager

# Constants and defaults
DB_PATH = os.environ.get('QUEUECTL_DB', os.path.expanduser('~/.queuectl/jobs.db'))
PID_FILE = os.environ.get('QUEUECTL_PID', os.path.expanduser('~/.queuectl/queuectl.pid'))
MASTER_LOG = os.environ.get('QUEUECTL_LOG', os.path.expanduser('~/.queuectl/queuectl.log'))
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 2
CLAIM_POLL_INTERVAL = 1.0  # seconds

# Ensure directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Utility helpers
def now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()
# Initialize DB schema
def init_db():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.executescript('''
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            worker_id TEXT,
            last_exit_code INTEGER,
            next_run REAL
        );

        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        ''')
        # set defaults if not present
        cur.execute('INSERT OR IGNORE INTO config(key,value) VALUES(?,?)', ('max_retries', str(DEFAULT_MAX_RETRIES)))
        cur.execute('INSERT OR IGNORE INTO config(key,value) VALUES(?,?)', ('backoff_base', str(DEFAULT_BACKOFF_BASE)))

# Config helpers
def config_get(key, default=None):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('SELECT value FROM config WHERE key=?', (key,))
        r = cur.fetchone()
        return r[0] if r else default

def config_set(key, value):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('INSERT OR REPLACE INTO config(key,value) VALUES(?,?)', (key, str(value)))

# Job API

def enqueue_job(job_json):
    try:
        data = json.loads(job_json)
    except Exception as e:
        print('Invalid JSON for job:', e, file=sys.stderr)
        return 2

    # Ensure required fields
    job_id = data.get('id') or str(uuid.uuid4())
    command = data.get('command')
    if not command:
        print('Job must include a "command" field', file=sys.stderr)
        return 2
    max_retries = int(data.get('max_retries') or config_get('max_retries') or DEFAULT_MAX_RETRIES)
    created_at = data.get('created_at') or now_iso()
    updated_at = data.get('updated_at') or created_at

    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute('INSERT INTO jobs(id,command,state,attempts,max_retries,created_at,updated_at,next_run) VALUES(?,?,?,?,?,?,?,?)',
                        (job_id, command, 'pending', 0, max_retries, created_at, updated_at, 0.0))
            print('Enqueued job', job_id)
            return 0
        except sqlite3.IntegrityError:
            print('Job with id already exists:', job_id, file=sys.stderr)
            return 3

# Listing / status

def list_jobs(state=None, limit=100):
    with get_conn() as conn:
        cur = conn.cursor()
        if state:
            cur.execute('SELECT * FROM jobs WHERE state=? ORDER BY created_at LIMIT ?', (state, limit))
        else:
            cur.execute('SELECT * FROM jobs ORDER BY created_at LIMIT ?', (limit,))
        rows = cur.fetchall()
        return [dict(r) for r in rows]

def status():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state")
        groups = {r['state']: r['cnt'] for r in cur.fetchall()}
        # active workers from pid file
        workers = 0
        master_pid = read_master_pid()
        if master_pid and pid_is_running(master_pid):
            workers = int(config_get('worker_count') or 0)
        return groups, workers

# DLQ operations

def dlq_list():
    return list_jobs(state='dead')

def dlq_retry(job_id):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM jobs WHERE id=?', (job_id,))
        r = cur.fetchone()
        if not r:
            print('Job not found', file=sys.stderr); return 2
        if r['state'] != 'dead':
            print('Job is not in DLQ (state is {})'.format(r['state']), file=sys.stderr); return 3
        now = now_iso()
        cur.execute('UPDATE jobs SET state=?, attempts=?, updated_at=?, next_run=? WHERE id=?', ('pending', 0, now, 0.0, job_id))
        print('Job retried:', job_id)
        return 0

# Atomic claim a job
def claim_job(worker_id):
    # find a pending job whose next_run <= now and atomically mark it processing and increment attempts
    with get_conn() as conn:
        cur = conn.cursor()
        now_ts = time.time()
        # Use a transaction to avoid races
        cur.execute('BEGIN IMMEDIATE')
        cur.execute('''SELECT id FROM jobs WHERE state='pending' AND (next_run IS NULL OR next_run<=?) ORDER BY created_at LIMIT 1''', (now_ts,))
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        job_id = row['id']
        # increment attempts and set processing
        cur.execute('SELECT attempts,max_retries FROM jobs WHERE id=?', (job_id,))
        meta = cur.fetchone()
        attempts = meta['attempts'] + 1
        cur.execute('UPDATE jobs SET state=?, worker_id=?, attempts=?, updated_at=? WHERE id=?',
                    ('processing', worker_id, attempts, now_iso(), job_id))
        conn.commit()
        cur.execute('SELECT * FROM jobs WHERE id=?', (job_id,))
        return dict(cur.fetchone())

# Worker loop

def run_command(command, timeout=None):
    try:
        # Use shell so we can run simple commands like sleep 2 or echo hi
        proc = subprocess.run(command, shell=True)
        return proc.returncode
    except FileNotFoundError:
        return 127
    except Exception as e:
        print('Execution error:', e, file=sys.stderr)
        return 1


def handle_job_result(job, exit_code):
    job_id = job['id']
    attempts = job['attempts']
    max_retries = job['max_retries']
    base = float(config_get('backoff_base') or DEFAULT_BACKOFF_BASE)
    with get_conn() as conn:
        cur = conn.cursor()
        now = now_iso()
        if exit_code == 0:
            cur.execute('UPDATE jobs SET state=?, last_exit_code=?, updated_at=?, worker_id=NULL WHERE id=?',
                        ('completed', exit_code, now, job_id))
            print('[{}] Job {} completed'.format(now, job_id))
            return
        # failed
        if attempts >= max_retries:
            # move to dead
            cur.execute('UPDATE jobs SET state=?, last_exit_code=?, updated_at=?, worker_id=NULL WHERE id=?',
                        ('dead', exit_code, now, job_id))
            print('[{}] Job {} moved to DLQ (attempts={} max={})'.format(now, job_id, attempts, max_retries))
        else:
            # schedule retry with exponential backoff
            delay = (base ** attempts)
            next_run = time.time() + delay
            cur.execute('UPDATE jobs SET state=?, last_exit_code=?, updated_at=?, worker_id=NULL, next_run=? WHERE id=?',
                        ('pending', exit_code, now, next_run, job_id))
            print('[{}] Job {} failed (exit={}), will retry in {}s (attempt {}/{})'.format(now, job_id, exit_code, int(delay), attempts, max_retries))

# Worker process function

def worker_process(stop_event, worker_name):
    print('[worker {}] started'.format(worker_name))
    while not stop_event.is_set():
        job = claim_job(worker_name)
        if not job:
            time.sleep(CLAIM_POLL_INTERVAL)
            continue
        print('[worker {}] picked job {}'.format(worker_name, job['id']))
        exit_code = run_command(job['command'])
        handle_job_result(job, exit_code)
    print('[worker {}] exiting'.format(worker_name))

# Master process to manage N workers
def master_run(count):
    # write master PID
    master_pid = os.getpid()
    with open(PID_FILE, 'w') as f:
        f.write(str(master_pid))
    config_set('worker_count', count)
    stop_event = multiprocessing.Event()

    procs = []
    def _sigint(signum, frame):
        print('Master received stop signal, shutting down...')
        stop_event.set()

    signal.signal(signal.SIGTERM, _sigint)
    signal.signal(signal.SIGINT, _sigint)

    try:
        for i in range(count):
            pname = f'worker-{i+1}'
            p = multiprocessing.Process(target=worker_process, args=(stop_event, pname), daemon=False)
            p.start()
            procs.append(p)
        print(f'Master started {len(procs)} workers (master pid={master_pid}). Ctrl-C to stop or use "queuectl worker stop"')
        # wait for children
        while any(p.is_alive() for p in procs):
            time.sleep(0.5)
    finally:
        stop_event.set()
        for p in procs:
            if p.is_alive():
                p.join(timeout=5)
        try:
            os.remove(PID_FILE)
        except Exception:
            pass

# PID helpers

def read_master_pid():
    try:
        with open(PID_FILE, 'r') as f:
            return int(f.read().strip())
    except Exception:
        return None

def pid_is_running(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True

def worker_stop():
    pid = read_master_pid()
    if not pid:
        print('No master running (pid file not found)')
        return 2
    try:
        os.kill(pid, signal.SIGTERM)
        print('Sent termination to master pid', pid)
        return 0
    except Exception as e:
        print('Failed to stop master:', e, file=sys.stderr)
        return 3

# CLI

def cmd_enqueue(args):
    return enqueue_job(args.job_json)


def cmd_worker_start(args):
    count = int(args.count or 1)
    # start master in foreground
    print('Starting master with {} workers...'.format(count))
    master_run(count)
    return 0


def cmd_worker_stop(args):
    return worker_stop()


def cmd_status(args):
    groups, workers = status()
    print('Workers active:', workers)
    print('Jobs by state:')
    for s in ['pending','processing','completed','failed','dead']:
        if s in groups:
            print(f'  {s}: {groups[s]}')
    # other states
    for k,v in groups.items():
        if k not in ['pending','processing','completed','failed','dead']:
            print(f'  {k}: {v}')
    return 0


def cmd_list(args):
    rows = list_jobs(state=args.state)
    for r in rows:
        print(json.dumps(r))
    return 0


def cmd_dlq_list(args):
    rows = dlq_list()
    for r in rows:
        print(json.dumps(r))
    return 0


def cmd_dlq_retry(args):
    return dlq_retry(args.job_id)


def cmd_config_set(args):
    config_set(args.key, args.value)
    print('Set', args.key, 'to', args.value)
    return 0


def cmd_config_get(args):
    val = config_get(args.key)
    print(args.key, '=', val)
    return 0

# Minimal tests script

def run_self_test():
    print('Running minimal self-test...')
    # enqueue a quick job and a failing job
    enqueue_job(json.dumps({'id':'test-echo','command':'echo hello','max_retries':2}))
    enqueue_job(json.dumps({'id':'test-fail','command':'bash -c "exit 5"','max_retries':2}))
    print('Start one worker for 5 seconds to process jobs...')
    p = Process(target=master_run, args=(1,), daemon=False)
    p.start()
    time.sleep(6)
    try:
        pid = read_master_pid()
        if pid:
            os.kill(pid, signal.SIGTERM)
    except Exception:
        pass
    p.join()
    print('Jobs after run:')
    for j in list_jobs():
        print(j['id'], j['state'], j['attempts'])

# Argument parsing

def build_parser():
    p = argparse.ArgumentParser(prog='queuectl', description='CLI job queue with workers, retries & DLQ')
    sub = p.add_subparsers(dest='cmd')

    ep = sub.add_parser('enqueue', help='Enqueue a job (JSON)')
    ep.add_argument('job_json')
    ep.set_defaults(func=cmd_enqueue)

    wp = sub.add_parser('worker', help='Worker management')
    wsub = wp.add_subparsers(dest='sub')
    wstart = wsub.add_parser('start', help='Start worker master and workers')
    wstart.add_argument('--count', type=int, default=1)
    wstart.set_defaults(func=cmd_worker_start)
    wstop = wsub.add_parser('stop', help='Stop workers (signal master)')
    wstop.set_defaults(func=cmd_worker_stop)

    sp = sub.add_parser('status', help='Show job stats and workers')
    sp.set_defaults(func=cmd_status)

    lp = sub.add_parser('list', help='List jobs')
    lp.add_argument('--state', choices=['pending','processing','completed','dead'], default=None)
    lp.set_defaults(func=cmd_list)

    dlp = sub.add_parser('dlq', help='DLQ operations')
    dsub = dlp.add_subparsers(dest='sub')
    dlist = dsub.add_parser('list', help='List DLQ jobs')
    dlist.set_defaults(func=cmd_dlq_list)
    dretry = dsub.add_parser('retry', help='Retry a DLQ job')
    dretry.add_argument('job_id')
    dretry.set_defaults(func=cmd_dlq_retry)

    cp = sub.add_parser('config', help='Get/set config')
    csub = cp.add_subparsers(dest='sub')
    cset = csub.add_parser('set', help='Set config key')
    cset.add_argument('key')
    cset.add_argument('value')
    cset.set_defaults(func=cmd_config_set)
    cget = csub.add_parser('get', help='Get config key')
    cget.add_argument('key')
    cget.set_defaults(func=cmd_config_get)

    t = sub.add_parser('selftest', help='Run a minimal self test')
    t.set_defaults(func=lambda args: run_self_test())

    return p

# Entry point
if __name__ == '__main__':
    init_db()
    parser = build_parser()
    args = parser.parse_args()
    if not args.cmd:
        parser.print_help(); sys.exit(0)
    sys.exit(args.func(args))
