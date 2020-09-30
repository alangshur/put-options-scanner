from src.scanner.wheel import WheelScanner
import os


if __name__ == '__main__':

    # scanner = EquityHistoryScanner(
    #     uni_file='universe/test-symbols.csv',
    #     num_processes=6
    # )

    # scanner = OptionCreditPutSpreadScanner(
    #     uni_file='universe/test-symbols.csv', 
    #     num_processes=6
    # )

    scanner = WheelScanner(
        uni_file='universe/active-symbols.csv', 
        num_processes=6,
        scan_name='wheel-active-1'
    )

    results = scanner.run()
    print(results)