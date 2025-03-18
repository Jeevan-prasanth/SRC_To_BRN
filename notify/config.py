import smtplib
import datetime as dt
from typing import List
import os
import sys

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

GMAIL_EMAIL = "alertproconnect@sedintechnologies.com"
GMAIL_PASSWORD = "xchy oofg qclb nzea"

ENV = "Demo"

recipients = ["suresh.parameswaran92@gmail.com",
              "suresh@sedintechnologies.com",
              "kannabiran@sedintechnologies.com"]

more_recipients = []

subject = "Audit Log for " + dt.date.today().strftime("%a %b %d") + " " + ENV

body_text = """Hi team,

PFA audit log for today

"""

# with open("notify/template.html", "r") as f:
#     body_html = f.read()

template = os.path.join(getattr(sys, "_MEIPASS", os.path.abspath(".")),"notify/template.html")

with open(template, "r") as f:
    body_html = f.read()

def send_email(
    subject: str,
    recipients: List[str] = recipients,
    body_text: str = "",
    body_html: str = "",
    file_name: str = None,
) -> None:
    """Send email to recipients"""
    # Header
    msg = MIMEMultipart("alternative")
    msg["From"] = f"ETL Alert {ENV} <{GMAIL_EMAIL}>"
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject

    # Body
    part1 = MIMEText(body_text, "plain")
    msg.attach(part1)

    if body_html:
        part2 = MIMEText(body_html, "html")
        msg.attach(part2)

    if file_name:
        with open(file_name, "rb") as f:
            part3 = MIMEApplication(f.read(), Name=file_name.split("/")[-1])
            msg.attach(part3)

    smtp_server, port = "smtp.gmail.com", 465
    with smtplib.SMTP_SSL(smtp_server, port) as smtp:
        smtp.login(GMAIL_EMAIL, GMAIL_PASSWORD)
        smtp.send_message(msg)


# optional quote
# r=requests.get('https://zenquotes.io/api/today')
# data=r.json()
# quote=data[0]['q']
# author=data[0]['a']

# text=f'{text}\n\n"{quote}"\n- {author}'
