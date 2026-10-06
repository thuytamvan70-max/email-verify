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

# 获取分片参数
shard_id = int(sys.argv[1])
total_shards = int(sys.argv[2])

def get_random_sender():
    username = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
    domain = random.choice(['gmail.com', 'outlook.com', 'yahoo.com', 'hotmail.com'])
    return f"{username}@{domain}"

def verify_email(email):
    for attempt in range(2):
        try:
            domain = email.split('@')[1]
            records = dns.resolver.resolve(domain, 'MX')
            mx_record = str(records[0].exchange)
            server = smtplib.SMTP(timeout=15)
            server.connect(mx_record, 25)
            server.helo('example.com')
            server.mail(get_random_sender()) # 随机发件人
            code, message = server.rcpt(email)
            server.quit()
            if code == 250:
                return f"{email} -> 有效"
            else:
                return f"{email} -> 无效 (代码: {code})"
        except smtplib.SMTPServerDisconnected:
            time.sleep(2)
            if attempt == 1: return f"{email} -> 限流断连"
        except Exception as e:
            return f"{email} -> 无法验证 ({str(e)[:50]})"

def task(email):
    time.sleep(random.uniform(0.1, 0.5))
    res = verify_email(email)
    with lock:
        print(res)
        results.append(res)

# 读取全量并切片
with open('emails.txt', 'r') as f:
    all_emails = [line.strip() for line in f if line.strip()]

# 每个机器只取属于自己的那份
my_emails = all_emails[shard_id-1::total_shards]

print(f"节点 {shard_id}/{total_shards} 开始验证 {len(my_emails)} 个邮箱...")
start_time = time.time()

with ThreadPoolExecutor(max_workers=20) as executor:
    executor.map(task, my_emails)

end_time = time.time()
total_time = end_time - start_time
speed = len(my_emails) / total_time if total_time > 0 else 0

# 写结果和速度报告
with open(f'result_{shard_id}.txt', 'w') as f:
    for res in results:
        f.write(res + '\n')
    f.write(f"\n=== 节点 {shard_id} 速度报告 ===\n")
    f.write(f"总数: {len(my_emails)} | 耗时: {total_time:.2f}秒 | 速度: {speed:.2f} 个/秒\n")

print(f"节点 {shard_id} 完成！速度: {speed:.2f} 个/秒")
