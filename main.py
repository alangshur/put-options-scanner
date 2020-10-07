from src.scanner.wheel import WheelScanner
import os


if __name__ == '__main__':
    scanner = WheelScanner(
        uni_file='universe/active-equities.csv',
        num_processes=6,
        save_scan=True,
        log_changes=True,
        scan_name='wheel-equities-1'
    )

    results = scanner.run()
    print('Fetch errors: {}'.format(results['fetch_failure_count']))    
    print('Fetch errors: {}'.format(results['analysis_failure_count']))