from src.scanner.cps import OptionCreditPutSpreadScanner
from src.scanner.equity import EquityHistoryScanner
import os


if __name__ == '__main__':

    scanner = EquityHistoryScanner(
        uni_file='universe/test-symbols.csv',
        num_processes=6
    )

    # scanner = OptionCreditPutSpreadScanner(
    #     uni_file='universe/test-symbols.csv', 
    #     num_processes=6
    # )

    results = scanner.run()
    print(results)