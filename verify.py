# -*- coding: utf-8 -*-
import sys, os, time, socket, sqlite3
import threading, queue, random, string
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_FILE  = BASE_DIR / "progress.db"
IN_FILE  = BASE_DIR / "emails.txt"

SMTP_MAP = {
    "outlook.com": "outlook-com.olc.protection.outlook.com",
    "hotmail.com": "hotmail-com.olc.protection.outlook.com",
    "live.com":    "live-com.olc.protection.outlook.com",
    "msn.com":     "msn-com.olc.protection.outlook.com",
}
SMTP_PORT = 25
USE_LIMIT = 5
PROBE_TIMEOUT = 10

def random_ehlo():
    rand_part= "".join(random.choices(string.ascii_lowercase, k=6))
    return f"{rand_part}.com"

# === 修复：永远返回本地 IP，不会因为一次 unknown 就退出 ===
class LocalIPManager:
    def __init__(self):
        pass
    def acquire(self, timeout=30):
        return ("local_ip", 0) # 永远返回本地IP

def db_init(path):
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.execute("CREATE TABLE IF NOT EXISTS results(email TEXT PRIMARY KEY, status TEXT)")
    conn.commit()
    return conn

def db_load_done(conn):
    return {r[0] for r in conn.execute("SELECT email FROM results").fetchall()}

def db_save(conn, rows, lock):
    with lock:
        conn.executemany("INSERT OR REPLACE INTO results(email,status) VALUES(?,?)", rows)
        conn.commit()

def verify_email(email, proxy, timeout):
    domain = email.split("@")[-1].lower() if "@" in email else ""
    smtp_host = SMTP_MAP.get(domain)
    if not smtp_host: return "skip"

    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((smtp_host, SMTP_PORT))

        banner = s.recv(1024).decode(errors="ignore")
        if not banner.startswith("220"): return "unknown"

        s.send(f"EHLO {random_ehlo()}\r\n".encode())
        if "250" not in s.recv(4096).decode(errors="ignore"): return "unknown"

        s.send(b"MAIL FROM:<test@test.de>\r\n")
        if not s.recv(1024).decode(errors="ignore").startswith("250"): return "unknown"

        s.send(f"RCPT TO:<{email}>\r\n".encode())
        rcpt = s.recv(1024).decode(errors="ignore")

        try: s.send(b"QUIT\r\n")
        except: pass

        if rcpt.startswith("250"): return "exist"
        if rcpt.startswith(("550","551","553")): return "fail"
        return "unknown"
    except Exception:
        return "unknown"
    finally:
        if s:
            try: s.close()
            except: pass

def main():
    if not IN_FILE.exists():
        print(f"[错误] 找不到输入文件: {IN_FILE}")
        return

    conn = db_init(DB_FILE)
    db_lock = threading.Lock()
    done_set = db_load_done(conn)

    def email_iter():
        with open(str(IN_FILE), encoding="utf-8-sig", errors="ignore") as f:
            for ln in f:
                e = ln.strip().lstrip("\ufeff").rstrip("\r\n")
                if e and "@" in e and e.lower() not in done_set:
                    yield e

    total_todo = 0
    with open(str(IN_FILE), encoding="utf-8-sig", errors="ignore") as f:
        for ln in f:
            e = ln.strip().lstrip("\ufeff").rstrip("\r\n")
            if e and "@" in e:
                total_todo += 1
    total_todo -= len(done_set)

    if total_todo <= 0:
        print("全部已验证完毕！")
        return

    n_threads = 20
    timeout_s = 10
    stop_ev = threading.Event()
    mgr = LocalIPManager()

    exist_n = [0]; fail_n = [0]; done_n = [0]; uk_n = [0]
    pend = []
    pend_lk = threading.Lock()
    task_q = queue.Queue(maxsize=n_threads * 3)

    def _producer():
        for email in email_iter():
            if stop_ev.is_set(): break
            task_q.put(email)
        for _ in range(n_threads):
            task_q.put(None)
    threading.Thread(target=_producer, daemon=True).start()

    def _worker():
        my_ip = mgr.acquire(timeout=10)
        use_cnt = 0
        while not stop_ev.is_set():
            try:
                email = task_q.get(timeout=2)
            except queue.Empty:
                continue
            if email is None:
                task_q.put(None)
                task_q.task_done()
                break

            status = verify_email(email, my_ip, timeout_s)

            # 【核心修复】遇到 unknown，停止 0.5 秒，继续用同一个 IP 跑下一个
            # 绝对不能丢弃 my_ip，否则线程就会因为没有 IP 自杀。
            if status == "unknown":
                time.sleep(random.uniform(0.5, 1.5)) 
                with pend_lk:
                    uk_n[0] += 1
                    done_n[0] += 1
                    idx = done_n[0]
                print(f"[{idx}/{total_todo}] {email} -> unknown (被限流/超时，休息重试)")
                task_q.task_done()
                continue

            use_cnt += 1

            if status == "fail":
                with pend_lk:
                    fail_n[0] += 1
                    done_n[0] += 1
                    idx = done_n[0]
                print(f"[{idx}/{total_todo}] {email} -> fail")
                task_q.task_done()
                continue

            if status == "exist":
                with pend_lk:
                    pend.append((email.lower(), "exist"))
                    exist_n[0] += 1
                    done_n[0]  += 1
                    idx = done_n[0]
                print(f"[{idx}/{total_todo}] {email} -> exist")
                task_q.task_done()
                continue
            task_q.task_done()

    def _auto_flush():
        while not stop_ev.is_set():
            time.sleep(5)
            with pend_lk:
                if pend:
                    rows = pend[:]
                    pend.clear()
                else: rows = []
            if rows:
                try: db_save(conn, rows, db_lock)
                except: pass

    threading.Thread(target=_auto_flush, daemon=True).start()
    
    threads = []
    for _ in range(n_threads):
        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    stop_ev.set()
    with pend_lk:
        if pend: db_save(conn, pend[:], db_lock)

    print("\n" + "="*50)
    print(f"验证完成: exist={exist_n[0]} fail={fail_n[0]} unknown={uk_n[0]}")
    shard_id = sys.argv[1] if len(sys.argv) > 1 else "1"
    with open(f'result_{shard_id}.txt', 'w', encoding='utf-8') as f:
        f.write(f"节点 {shard_id}\n总数: {done_n[0]} | 有效: {exist_n[0]} | 无效: {fail_n[0]} | 未知: {uk_n[0]}\n")
        for r in conn.execute("SELECT email, status FROM results ORDER BY status"):
            f.write(f"{r[0]} -> {r[1]}\n")
    conn.close()

if __name__ == "__main__":
    main()
