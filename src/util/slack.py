from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import time
import os
from dotenv import load_dotenv
load_dotenv(verbose=True)


class SlackTextSender:

    def __init__(self):

        # get slack client
        self.client = WebClient(token=os.environ['SLACK_BOT_TOKEN'])
        
    def send_message(self, subject, text):
        print('sending!')

        # submit slack message
        try:
            message = '\n{}\n\n{}'.format(subject, text)
            response = self.client.chat_postMessage(
                channel=os.environ['SLACK_BOT_CHANNEL'], 
                text=message
            )
            assert response['message']['text'] == message
            
        except SlackApiError as e:
            assert e.response['ok'] is False
            assert e.response['error']

        print('done')