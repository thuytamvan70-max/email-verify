import smtplib
import dns.resolver
import threading
import time
import random
import string
import sys
from concurrent.futures import ThreadPoolExecutor

lock = threading.Lock()
results = []

# DNS 缓存，避免重复查询
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

def get_random_sender():
    username = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
    return f"{username}@gmail.com"

def verify_email(email):
    try:
        domain = email.split('@')[1]
        mx_record = get_mx(domain)
        if not mx_record:
            return f"{email} -> 无效 (无MX记录)"

        server = smtplib.SMTP(timeout=5)  # 超时时间降到 5 秒，快速失败
        server.connect(mx_record, 25)
        server.helo('example.com')
        server.mail(get_random_sender())
        code, message = server.rcpt(email)
        server.quit()
        
        if code == 250:
            return f"{email} -> 有效"
        else:
            return f"{email} -> 无效 (代码: {code})"
    except Exception as e:
        return f"{email} -> 无法验证 ({str(e)[:30]})"

def task(email):
    res = verify_email(email)
    with lock:
        print(res)
        results.append(res)

with open('emails.txt', 'r') as f:
    all_emails = [line.strip() for line in f if line.strip()]

shard_id = int(sys.argv[1])
total_shards = int(sys.argv[2])
my_emails = all_emails[shard_id-1::total_shards]

print(f"节点 {shard_id}/{total_shards} 开始暴力验证 {len(my_emails)} 个邮箱...")
start_time = time.time()

# 并发直接拉满到 100
with ThreadPoolExecutor(max_workers=100) as executor:
    executor.map(task, my_emails)

end_time = time.time()
total_time = end_time - start_time
speed = len(my_emails) / total_time if total_time > 0 else 0

with open(f'result_{shard_id}.txt', 'w') as f:
    for res in results:
        f.write(res + '\n')
    f.write(f"\n=== 节点 {shard_id} 速度报告 ===\n")
    f.write(f"总数: {len(my_emails)} | 耗时: {total_time:.2f}秒 | 速度: {speed:.2f} 个/秒\n")

print(f"节点 {shard_id} 完成！速度: {speed:.2f} 个/秒")
