import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv
load_dotenv(verbose=True)


class MailTextSender:

    def __init__(self, server_cold_start=False):

        self.server_cold_start = server_cold_start

        # get email targets
        self.sender_email = os.environ.get('MAIL_SENDER_EMAIL')
        self.receiver_email = os.environ.get('MAIL_RECEIVER_EMAIL')

        # get environment variables
        self.smtp_server = os.environ.get('MAIL_SMTP_SERVER')
        self.port = os.environ.get('MAIL_PORT')
        self.password = os.environ.get('MAIL_PASSWORD')
        
        # build smpt server connection
        if not self.server_cold_start: 
            self.context = ssl.create_default_context()
            self.server = smtplib.SMTP_SSL(self.smtp_server, self.port, context=self.context)
            self.server.login(self.sender_email, self.password)

    def __del__(self):

        # close server connection
        self.server.close()
        
    def send_message(self, subject, text):

        if self.server_cold_start:
            context = ssl.create_default_context()
            server = smtplib.SMTP_SSL(self.smtp_server, self.port, context=context)
            server.login(self.sender_email, self.password)
        else:
            server = self.server

        # build mime message
        message = MIMEMultipart('alternative')
        message['Subject'] = subject
        message['From'] = self.sender_email
        message['To'] = self.receiver_email

        # compile text
        text_plain = MIMEText('\n\n' + text + '  ', 'plain')
        message.attach(text_plain)

        # send message
        server.sendmail(
            self.sender_email, 
            self.receiver_email, 
            message.as_string()
        )