import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv
load_dotenv(verbose=True)


class MailTextSender:

    def __init__(self):

        # get email targets
        self.sender_email = os.environ.get('MAIL_SENDER_EMAIL')
        self.receiver_email = os.environ.get('MAIL_RECEIVER_EMAIL')

        # get environment variables
        smtp_server = os.environ.get('MAIL_SMTP_SERVER')
        port = os.environ.get('MAIL_PORT')
        password = os.environ.get('MAIL_PASSWORD')
        
        # build smpt server connection
        self.context = ssl.create_default_context()
        self.server = smtplib.SMTP_SSL(smtp_server, port, context=self.context)
        self.server.login(self.sender_email, password)

    def __del__(self):

        # close server connection
        self.server.close()

    def send_message(self, subject, text):

        # build mime message
        message = MIMEMultipart('alternative')
        message['Subject'] = subject
        message['From'] = self.sender_email
        message['To'] = self.receiver_email

        # compile text
        text_plain = MIMEText(text, 'plain')
        message.attach(text_plain)

        # send message
        server.sendmail(
            sender_email, 
            receiver_email, 
            message.as_string()
        )