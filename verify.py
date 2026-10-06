import smtplib
import dns.resolver
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor

lock = threading.Lock()
results = []

def verify_email(email):
    # 重试机制：最多试 2 次
    for attempt in range(2):
        try:
            domain = email.split('@')[1]
            records = dns.resolver.resolve(domain, 'MX')
            mx_record = str(records[0].exchange)

            server = smtplib.SMTP(timeout=15)
            server.connect(mx_record, 25)
            server.helo('example.com')
            server.mail('test@example.com')
            
            code, message = server.rcpt(email)
            server.quit()
            
            if code == 250:
                return f"{email} -> 有效 (或防枚举虚假返回)"
            else:
                return f"{email} -> 无效 (代码: {code})"
                
        except smtplib.SMTPServerDisconnected:
            # 遇到被服务器断连，休息一下再重试
            time.sleep(2)
            if attempt == 1:
                return f"{email} -> 验证失败 (被微软限流断连)"
        except Exception as e:
            return f"{email} -> 无法验证 (错误: {str(e)[:50]})"

def task(email):
    # 每个线程开始前，随机休息 0.1 到 0.5 秒，避免并发冲击
    time.sleep(random.uniform(0.1, 0.5))
    res = verify_email(email)
    with lock:
        print(res)
        results.append(res)

with open('emails.txt', 'r') as f:
    emails = [line.strip() for line in f if line.strip()]

print(f"开始验证 {len(emails)} 个邮箱，控制并发数为 3...")
# 极其重要的改动：把 20 降成 3
with ThreadPoolExecutor(max_workers=3) as executor:
    executor.map(task, emails)

with open('result.txt', 'w') as f:
    for res in results:
        f.write(res + '\n')

print("验证完成！")
