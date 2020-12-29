from src.executor.scan import ScanExecutor
from src.executor.portfolio import PortfolioExecutor
from src.util.mail import MailTextSender
import datetime as dt
import time
import os


class LoopMonitorExecutor:

    def __init__(self, 
                 delay_secs=900, 
                 repeat_window=3600, 
                 score_threshold=50):

        self.delay_secs = delay_secs
        self.repeat_window = repeat_window
        self.score_threshold = score_threshold

        self.scan_executor = ScanExecutor()
        self.portfolio_executor = PortfolioExecutor()
        self.text_sender = MailTextSender(server_cold_start=True)
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
                now = time.time()
                put_scan = self.scan_executor.run_put_scanner(
                    ignore_active_tickers=False,
                    print_results=True,
                    refresh_results=True,
                    return_results=True,
                    prog_bar=False
                )

                # scan portfolio
                portfolio_scan = self.portfolio_executor.run_portfolio_read(
                    print_general_stats=False,
                    return_results=True
                )

                # execute scans
                self.notify_contract_scan(put_scan, now)
                self.notify_portfolio_scan(put_scan, portfolio_scan, now)
                
                # sleep until next scan
                print()
                scan_num += 1
                then = time.time()
                diff = int(then - now)
                sleep_secs = self.delay_secs - diff
                while sleep_secs > 0:
                    print('\rNext scan in {} seconds...\t'.format(sleep_secs), end='')
                    time.sleep(1)
                    sleep_secs -= 1
                print('\rLoading scan {}...\t\t'.format(scan_num), end='')

        except KeyboardInterrupt:
            print('\nTerminating...')

    def notify_contract_scan(self, put_scan, now):

        # filter scan results
        if put_scan is None: return
        put_scan = put_scan.sort_values('score (%)', ascending=False)
        put_scan = put_scan[put_scan['score (%)'] >= self.score_threshold]
        cur_time = dt.datetime.now().strftime("%I:%M %p")
        for contract in put_scan.index:
            
            # verify repeated alert
            if contract in self.lifetime_notifications:
                last_update = self.lifetime_notifications[contract]
                if now - last_update > self.repeat_window: continue

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
            self.text_sender.send_message(subject, text)
            self.lifetime_notifications[contract] = now

    def notify_portfolio_scan(self, put_scan, portfolio_scan, now):

        # get puts to close
        closeable_puts = portfolio_scan[
            portfolio_scan['cur_a_roc (%)'] >= portfolio_scan['a_roc (%)']
        ].index

        # iterate over closeable puts
        cur_time = dt.datetime.now().strftime("%I:%M %p")
        for contract in closeable_puts:

            # verify repeated alert
            if contract in self.lifetime_notifications:
                last_update = self.lifetime_notifications[contract]
                if now - last_update > self.repeat_window: continue
        
            # draft alert messages
            ticker = str(contract).split(' ')[0]
            subject = 'ALERT: {} Closeable Put'.format(ticker)
            text = 'Portfolio scan at {} revealed a closeable opportunity in {} at a return of {} and an annualized return-on-capital of {}.'.format(
                cur_time,
                contract,
                str(portfolio_scan.loc[contract, 'return (%)']) + '%',
                str(portfolio_scan.loc[contract, 'cur_a_roc (%)']) + '%'
            )

            # send alert
            self.text_sender.send_message(subject, text)
            self.lifetime_notifications[contract] = now