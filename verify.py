import smtplib
import dns.resolver
import threading
import time
import sys
from concurrent.futures import ThreadPoolExecutor

lock = threading.Lock()
results = []
dns_cache = {}

def get_mx(domain):
    if domain in dns_cache: return dns_cache[domain]
    try:
        records = dns.resolver.resolve(domain, 'MX')
        mx = str(records[0].exchange)
        dns_cache[domain] = mx
        return mx
    except:
        dns_cache[domain] = None
        return None

def verify_email(email):
    try:
        domain = email.split('@')[1]
        mx_record = get_mx(domain)
        if not mx_record: return f"{email} -> 无效 (无MX记录)"

        server = smtplib.SMTP(timeout=3) # 超过 3 秒没响应直接放弃
        server.connect(mx_record, 25)
        server.helo('example.com')
        server.mail('verify@test.com')
        code, message = server.rcpt(email)
        server.quit()
        return f"{email} -> 有效" if code == 250 else f"{email} -> 无效 ({code})"
    except Exception as e:
        return f"{email} -> 无法验证"

def task(email):
    res = verify_email(email)
    with lock:
        print(res)
        results.append(res)

with open('emails.txt', 'r') as f:
    all_emails = [line.strip() for line in f if line.strip()]

shard_id = int(sys.argv[1])
my_emails = all_emails[shard_id-1::20]

print(f"节点 {shard_id}/20 开始极速验证 {len(my_emails)} 个邮箱...")
start_time = time.time()

# 极速并发：200线程
with ThreadPoolExecutor(max_workers=200) as executor:
    executor.map(task, my_emails)

total_time = time.time() - start_time
speed = len(my_emails) / total_time if total_time > 0 else 0

with open(f'result_{shard_id}.txt', 'w') as f:
    f.write('\n'.join(results))
    f.write(f"\n\n=== 节点 {shard_id} 极速报告 ===\n")
    f.write(f"数量: {len(my_emails)} | 耗时: {total_time:.2f}秒 | 速度: {speed:.2f} 个/秒\n")

print(f"节点 {shard_id} 完成！速度: {speed:.2f} 个/秒")
