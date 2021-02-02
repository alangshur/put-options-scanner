from src.executor.portfolio import PortfolioExecutor
from src.executor.scan import ScanExecutor
from src.util.slack import SlackTextSender
from tabulate import tabulate
from functools import reduce
import datetime as dt
import numpy as np
import time
import os


class LoopMonitorExecutor:

    def __init__(self, 
                 delay_secs=900, 
                 repeat_window=3600, 
                 score_threshold=50,
                 notify_activity=True):

        self.delay_secs = delay_secs
        self.repeat_window = repeat_window
        self.score_threshold = score_threshold
        self.notify_activity = notify_activity

        self.scan_executor = ScanExecutor()
        self.portfolio_executor = PortfolioExecutor()
        self.text_sender = SlackTextSender()
        self.lifetime_notifications = {}

    def num_to_float(self, num):
        if '$' in num: num = num[1:]
        if '.' in num: num = num[:-3]
        num = num.replace(',', '')
        num = float(num)
        return num

    def run(self):
        scan_num = 0

        try:
            while True:

                # scan targets contracts
                os.system('clear')
                print('Running put scan... ', end='', flush=True)
                now = time.time()
                put_scan = self.scan_executor.run_put_scanner(
                    ignore_active_tickers=False,
                    print_results=False,
                    refresh_results=False,
                    return_results=True,
                    prog_bar=False
                )

                # scan portfolio
                print('Done', flush=True)
                portfolio_scan = self.portfolio_executor.run_portfolio_read(
                    print_results=False,
                    print_general_stats=False,
                    return_results=True
                )

                # execute scans
                print('Running portfolio scan... ', end='', flush=True)
                put_scan = self.filter_put_scan(put_scan, portfolio_scan)
                self.notify_contract_scan(put_scan)
                self.notify_portfolio_scan(put_scan, portfolio_scan)

                # print results
                print('Done', flush=True)
                self.print_results(put_scan, portfolio_scan)
                
                # sleep until next scan
                print(flush=True)
                scan_num += 1
                then = time.time()
                diff = int(then - now)
                sleep_secs = self.delay_secs - diff
                while sleep_secs > 0:
                    print('\rNext scan in {} seconds...\t'.format(sleep_secs), end='', flush=True)
                    time.sleep(1)
                    sleep_secs -= 1
                print('\rLoading scan {}...\t\t'.format(scan_num), end='', flush=True)

        except KeyboardInterrupt:
            print('\nTerminating...')

    def filter_put_scan(self, put_scan, portfolio_scan):
        updated_put_scan = put_scan.copy()

        # get puts to avoid
        cur_a_roc = portfolio_scan.loc[:, 'cur_a_roc (%)'].astype(np.float32)
        a_roc = portfolio_scan.loc[:, 'a_roc (%)'].astype(np.float32)
        dte = portfolio_scan.loc[:, 'dte (D)'].astype(np.float32)
        mask = (cur_a_roc < a_roc) & (dte > 1)

        # strip ticker names
        contracts = portfolio_scan[mask].index.values.tolist()
        tickers = [c.split(' ')[0] for c in contracts]

        # filter put scan tickers
        if len(tickers) == 0: return put_scan
        masks = [put_scan.index.str.startswith(t) for t in tickers]
        mask = reduce(lambda x, y: x | y, masks)
        updated_put_scan = put_scan[~mask]
        
        return updated_put_scan 

    def notify_contract_scan(self, put_scan):

        # filter scan results
        if put_scan is None: return
        put_scan = put_scan.sort_values('score (%)', ascending=False)
        put_scan = put_scan[put_scan['score (%)'] >= self.score_threshold]
        cur_time = dt.datetime.now().strftime("%I:%M %p")
        for contract in put_scan.index:
            
            # verify repeated alert
            if contract in self.lifetime_notifications:
                last_update = self.lifetime_notifications[contract]
                if time.time() - last_update < self.repeat_window: continue

            # draft alert messages
            score = put_scan.loc[contract, 'score (%)']
            ticker = str(contract).split(' ')[0]
            subject = 'ALERT: {} High Score'.format(ticker)
            text = 'Put scan at {} revealed a score of {}% for the contract {}.'.format(
                cur_time, 
                round(score, 2), 
                contract
            )

            # send alert
            if self.notify_activity: self.text_sender.send_message(subject, text)
            self.lifetime_notifications[contract] = time.time()

    def notify_portfolio_scan(self, put_scan, portfolio_scan):

        # iterate over closeable puts
        cur_time = dt.datetime.now().strftime("%I:%M %p")
        for contract in portfolio_scan.index:

            # ignore puts with immature a_roc
            cur_a_roc = float(portfolio_scan.loc[contract, 'cur_a_roc (%)'])
            a_roc = float(portfolio_scan.loc[contract, 'a_roc (%)'])
            if cur_a_roc < a_roc:
                continue
                        
            # verify repeated alert
            if contract in self.lifetime_notifications:
                last_update = self.lifetime_notifications[contract]
                if time.time() - last_update < self.repeat_window: continue
        
            # draft alert messages
            ticker = str(contract).split(' ')[0]
            subject = 'ALERT: {} Closeable Put'.format(ticker)
            text = 'Portfolio scan at {} revealed a closeable opportunity in {} at a return of {} and an annualized return-on-capital of {}.'.format(
                cur_time,
                contract,
                str(portfolio_scan.loc[contract, 'return (%)']) + '%',
                str(portfolio_scan.loc[contract, 'cur_a_roc (%)']) + '%'
            )

            # send alerts
            if self.notify_activity: self.text_sender.send_message(subject, text)
            self.lifetime_notifications[contract] = time.time()

    def print_results(self, put_scan, portfolio_scan):
        formatted_put_scan = tabulate(put_scan, headers='keys', tablefmt='psql')
        formatted_portfolio_scan = tabulate(portfolio_scan, headers='keys', tablefmt='psql')

        print(flush=True)
        print('Contract Scan', flush=True)
        print(formatted_put_scan, flush=True)

        print(flush=True)
        print('Portfolio Scan', flush=True)
        print(formatted_portfolio_scan, flush=True)

