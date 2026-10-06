# -*- coding: utf-8 -*-
"""
微软邮箱验证 - GitHub Actions 定制版
- 移除了交互和 config.ini，适配无代理直连模式（默认使用微软主机IP）
- 支持 Outlook/Hotmail/Live 域名
- 保留了断点续传、IP池、熔断逻辑
- 线程数强制限制为 20，防止触发微软 450 熔断
"""
import sys, os, time, socket, sqlite3
import threading, queue, random, string
from pathlib import Path

try:
    import socks as _socks
except ImportError:
    _socks = None

BASE_DIR = Path(__file__).resolve().parent
DB_FILE  = BASE_DIR / "progress.db"
IN_FILE  = BASE_DIR / "emails.txt" # 前端上传的文件

# === 微软邮箱 MX 映射 ===
SMTP_MAP = {
    "outlook.com": "outlook-com.olc.protection.outlook.com",
    "hotmail.com": "hotmail-com.olc.protection.outlook.com",
    "live.com":    "live-com.olc.protection.outlook.com",
    "msn.com":     "msn-com.olc.protection.outlook.com",
}
SMTP_PORT = 25
USE_LIMIT = 5           # 每个IP最多用5次，防止触发熔断
PROBE_HOST = "outlook-com.olc.protection.outlook.com"
PROBE_TIMEOUT = 5
PROBE_THREADS = 20
BATCH_MAX_WAIT = 10

def random_ehlo():
    prefixes = ["mail","smtp","mx","send","host","relay"]
    suffix   = random.choice(["com","net","org"])
    rand_part= "".join(random.choices(string.ascii_lowercase, k=random.randint(4,8)))
    name = random.choice(prefixes)+rand_part if random.random()<0.5 else rand_part
    return f"{name}.{suffix}"

# === 无代理直连降级 ===
class LocalIPManager:
    """如果没有配置代理，直接返回本机IP（微软主机IP）"""
    def __init__(self):
        self.ready_q = queue.Queue()
        self.ready_q.put(("local_ip", 0))

    def acquire(self, timeout=30):
        try: return self.ready_q.get(timeout=timeout)
        except queue.Empty: return None

    def start_supplier(self): pass
    def _stop(self): return False

# === 数据库断点续传 ===
def db_init(path):
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.execute("""CREATE TABLE IF NOT EXISTS results(
        email TEXT PRIMARY KEY, status TEXT NOT NULL)""")
    conn.commit()
    return conn

def db_load_done(conn):
    return {r[0] for r in conn.execute("SELECT email FROM results").fetchall()}

def db_save(conn, rows, lock):
    with lock:
        conn.executemany("INSERT OR REPLACE INTO results(email,status) VALUES(?,?)", rows)
        conn.commit()

# === 验证核心逻辑 ===
def verify_email(email, proxy, timeout):
    domain = email.split("@")[-1].lower() if "@" in email else ""
    smtp_host = SMTP_MAP.get(domain)
    if not smtp_host:
        return "skip"

    host, port = proxy
    s = None
    try:
        if host == "local_ip" or _socks is None:
            # 直连模式（使用 GitHub 机器的微软 IP）
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect((smtp_host, SMTP_PORT))
        else:
            # SOCKS5 代理模式
            s = _socks.socksocket()
            s.set_proxy(_socks.SOCKS5, host, port)
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
        # 微软返回 450 通常是因为请求太频繁，标记为 unknown 进行重试或记录
        if rcpt.startswith(("450","451","452")): return "unknown"
        return "unknown"
    except Exception:
        return "unknown"
    finally:
        if s:
            try: s.close()
            except: pass

# === 主流程 ===
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

    n_threads = 20        # 关键降级：20并发，防止熔断
    timeout_s = 10
    stop_ev = threading.Event()

    # 如果你是代理用户，可以把你的 API 填在这里，目前默认为直连
    mgr = LocalIPManager() 

    exist_n = [0]; fail_n = [0]; skip_n = [0]; done_n = [0]; uk_n = [0]
    start_t = time.time()
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
            if my_ip is None:
                my_ip = mgr.acquire(timeout=5)
                if my_ip is None: break
            
            try:
                email = task_q.get(timeout=2)
            except queue.Empty:
                continue
            if email is None:
                task_q.put(None)
                task_q.task_done()
                break

            if use_cnt >= USE_LIMIT:
                mgr.ready_q.put(my_ip)
                my_ip = mgr.acquire(timeout=5)
                use_cnt = 0
                if my_ip is None: break

            status = verify_email(email, my_ip, timeout_s)

            if status == "unknown":
                my_ip = None # 丢弃本地IP重新获取
                with pend_lk:
                    uk_n[0] += 1
                    done_n[0] += 1
                    idx = done_n[0]
                print(f"[{idx}/{total_todo}] {email} -> unknown (限流/超时)")
                task_q.task_done()
                continue

            use_cnt += 1

            if status == "skip":
                skip_n[0] += 1
                task_q.task_done()
                continue

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

    # 等待线程结束
    for t in threads:
        t.join()

    stop_ev.set()
    with pend_lk:
        if pend:
            db_save(conn, pend[:], db_lock)

    # === 生成最终报告 ===
    print("\n" + "="*50)
    print(f"验证完成: exist={exist_n[0]} fail={fail_n[0]} skip={skip_n[0]} unknown={uk_n[0]}")
    
    shard_id = sys.argv[1] if len(sys.argv) > 1 else "1"
    with open(f'result_{shard_id}.txt', 'w', encoding='utf-8') as f:
        f.write(f"节点 {shard_id} 验证结果\n")
        f.write(f"总数: {done_n[0]} | 有效: {exist_n[0]} | 无效: {fail_n[0]} | 未知: {uk_n[0]}\n")
        f.write("-" * 30 + "\n")
        for r in conn.execute("SELECT email, status FROM results ORDER BY status"):
            f.write(f"{r[0]} -> {r[1]}\n")

    conn.close()

if __name__ == "__main__":
    main()
