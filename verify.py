import smtplib
import dns.resolver
import threading
from concurrent.futures import ThreadPoolExecutor

# 线程锁，防止多个线程同时写入文件发生错乱
lock = threading.Lock()
results = []

def verify_email(email):
    try:
        domain = email.split('@')[1]
        records = dns.resolver.resolve(domain, 'MX')
        mx_record = str(records[0].exchange)

        server = smtplib.SMTP(timeout=10)
        server.connect(mx_record, 25)
        server.helo('example.com')
        server.mail('test@example.com')
        
        code, message = server.rcpt(email)
        server.quit()
        
        if code == 250:
            return f"{email} -> 有效 (或服务器防枚举虚假返回)"
        else:
            return f"{email} -> 无效 (代码: {code})"
    except Exception as e:
        return f"{email} -> 无法验证 (错误: {str(e)[:50]})"

# 线程执行的任务
def task(email):
    res = verify_email(email)
    with lock:
        print(res)
        results.append(res)

# 读取邮箱
with open('emails.txt', 'r') as f:
    emails = [line.strip() for line in f if line.strip()]

# 开启多线程并发：这里设定最多同时跑 20 个任务
print(f"开始并发验证 {len(emails)} 个邮箱...")
with ThreadPoolExecutor(max_workers=20) as executor:
    executor.map(task, emails)

# 写入结果
with open('result.txt', 'w') as f:
    for res in results:
        f.write(res + '\n')

print("验证完成！")
