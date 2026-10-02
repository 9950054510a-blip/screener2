import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

def send_email_alert(subject: str, message: str) -> bool:
    sender = os.getenv("EMAIL_SENDER")
    password = os.getenv("APP_PASSWORD")  # Изменили имя здесь
    receiver = os.getenv("EMAIL_RECEIVER")
    host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    port = int(os.getenv("SMTP_PORT", 587))

    if not sender or not password or not receiver or "your_email" in sender:
        print("⚠️ Email-уведомления пропущены: не заполнены данные в файле .env")
        return False

    try:
        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = receiver
        msg["Subject"] = subject

        msg.attach(MIMEText(message, "plain"))

        with smtplib.SMTP(host, port) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, receiver, msg.as_string())

        print(f"📧 Email успешно отправлен на {receiver}")
        return True
    except Exception as e:
        print(f"❌ Ошибка отправки Email: {e}")
        return False
