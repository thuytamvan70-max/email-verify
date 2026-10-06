import smtplib
import dns.resolver

def verify_email(email):
    domain = email.split('@')[1]
    try:
        # 1. 查 MX 记录
        records = dns.resolver.resolve(domain, 'MX')
        mx_record = str(records[0].exchange)

        # 2. 连接 SMTP 服务器
        server = smtplib.SMTP(timeout=10)
        server.connect(mx_record, 25)
        server.helo('example.com')
        server.mail('test@example.com')
        
        # 3. 发送 RCPT TO 指令（核心验证）
        code, message = server.rcpt(email)
        server.quit()
        
        if code == 250:
            return f"{email} -> 有效 (或服务器防枚举虚假返回)"
        else:
            return f"{email} -> 无效 (代码: {code})"
    except Exception as e:
        return f"{email} -> 无法验证 (错误: {str(e)[:30]})"

with open('emails.txt', 'r') as f:
    emails = [line.strip() for line in f if line.strip()]

with open('result.txt', 'w') as f:
    for email in emails:
        res = verify_email(email)
        print(res)
        f.write(res + '\n')