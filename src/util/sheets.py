from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
import os
import pickle
from dotenv import load_dotenv
load_dotenv(verbose=True)


class SheetsPortfolioExtractor:

    def fetch(self, sheet_range):
        
        # get cached credentials
        creds = None
        if os.path.exists('auth/token.pickle'):
            with open('auth/token.pickle', 'rb') as token:
                creds = pickle.load(token)

        # authenticate remotely
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'auth/credentials.json', 
                    [os.environ.get('SHEETS_API_ENDPOINT')]
                )
                creds = flow.run_local_server(port=0)
            with open('auth/token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        # fetch target sheet
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=os.environ.get('PORTFOLIO_SHEET_ID'),
            range=sheet_range
        ).execute()

        # create dataframe
        values = result.get('values', [])
        if not values: return None
        else: return pd.DataFrame(values[1:], columns=values[0])