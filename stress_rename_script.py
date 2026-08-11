import sqlite3
import subprocess
import time
import os
import sys
import random

DB_NAME = "stress_rename_lock_test.db"
os.environ["GBD_DB"] = DB_NAME

if os.path.exists(DB_NAME):
    os.remove(DB_NAME)

# Bootstrap
subprocess.run([sys.executable, 'gbd.py', 'create', 'feat0', '-u', 'empty'], capture_output=True)

iterations = 80
locked_errors = 0
busy_errors = 0
other_errors = 0
info_ok_count = 0
created_features = ['feat0']

for i in range(1, iterations + 1):
    if i % 2 == 1:
        feat = f"f{i}"
        cmd = [sys.executable, 'gbd.py', 'create', feat, '-u', 'empty']
        created_features.append(feat)
    else:
        if len(created_features) > 0:
            src = created_features[-1]
            dst = f"{src}_ren"
            cmd = [sys.executable, 'gbd.py', 'rename', src, dst]
        else:
            cmd = [sys.executable, 'gbd.py', 'info']

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(random.uniform(0.005, 0.040))
    p.terminate()
    try:
        p.wait(timeout=0.2)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()

    try:
        conn = sqlite3.connect(DB_NAME, timeout=0.1)
        conn.execute("CREATE TABLE IF NOT EXISTS _probe2(i INTEGER);")
        conn.execute("INSERT INTO _probe2(i) VALUES (?);", (i,))
        conn.commit()
        conn.close()
    except sqlite3.OperationalError as e:
        msg = str(e).lower()
        if 'locked' in msg:
            locked_errors += 1
        elif 'busy' in msg:
            busy_errors += 1
        else:
            other_errors += 1
    except Exception:
        other_errors += 1

    if i % 10 == 0:
        res = subprocess.run([sys.executable, 'gbd.py', 'info'], capture_output=True)
        if res.returncode == 0:
            info_ok_count += 1

print(f"Summary: iterations={iterations}, locked={locked_errors}, busy={busy_errors}, other={other_errors}, info_ok_count={info_ok_count}")

if os.path.exists(DB_NAME):
    os.remove(DB_NAME)

if locked_errors > 0 or busy_errors > 0:
    sys.exit(1)
