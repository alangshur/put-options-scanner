from src.executor.scan import ScanExecutor
from src.util.mail import MailTextSender
import datetime as dt
import time
import os


class LoopScanExecutor:

    def __init__(self, 
                 delay_secs=900, 
                 repeat_window=3600, 
                 score_threshold=50):

        self.delay_secs = delay_secs
        self.repeat_window = repeat_window
        self.score_threshold = score_threshold

        self.scan_executor = ScanExecutor()
        self.text_sender = MailTextSender()
        self.lifetime_notifications = {}

    def run_scan_loop(self, verbose=True):
        scan_num = 0 

        try:
            while True:

                # get top scan results
                now = time.time()
                scan_results = self.scan_executor.run_put_scanner(
                    ignore_active_tickers=False,
                    print_results=True,
                    refresh_results=True,
                    return_results=True,
                    prog_bar=False
                )

                scan_results = scan_results.sort_values('score (%)', ascending=False)
                scan_results = scan_results[scan_results['score (%)'] >= self.score_threshold]
                cur_time = dt.datetime.now().strftime("%I:%M %p")
                for contract in scan_results.index:

                    # verify repeated alert
                    if contract in self.lifetime_notifications:
                        last_update = self.lifetime_notifications[contract]
                        if now - last_update > self.repeat_window: continue

                    # draft alert messages
                    score = scan_results.loc[contract, 'score (%)']
                    ticker = str(contract).split(' ')[0]
                    subject = 'ALERT: {} High Score'.format(ticker)
                    text = 'Put scan at {} revealed a score of {}% for the contract {}.'.format(cur_time, round(score, 2), contract)
                    
                    # send alert
                    self.text_sender.send_message(subject, text)
                    self.lifetime_notifications[contract] = now

                # sleep until next scan
                scan_num += 1
                then = time.time()
                diff = int(then - now)
                sleep_secs = self.delay_secs - diff
                while sleep_secs > 0:
                    print('\n\nScan {} in {} seconds...\t'.format(scan_num, sleep_secs), end='')
                    time.sleep(1)
                    sleep_secs -= 1

        except KeyboardInterrupt:
            print('\nTerminating...')
