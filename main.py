from src.scanner.equity import EquityScanner
from src.scanner.option import OptionScanner
from src.analyzer.regression import RegressionAnalyzer
from src.analyzer.cps import CreditPutSpreadAnalyzer
import os


if __name__ == '__main__':

    # analyzer = RegressionAnalyzer()
    # scanner = EquityScanner(
    #     uni_file='universe/active-equities.csv',
    #     analyzer=analyzer,
    #     num_processes=12
    # )
    # results = scanner.run()
    # print(results)

    analyzer = CreditPutSpreadAnalyzer()
    scanner = OptionScanner(
        uni_file='universe/test-symbols.csv', 
        analyzer=analyzer,
        num_processes=6
    )

    results = scanner.run()
    print('Fetching errors: {}'.format(results['fetch_failure_count']))
    print('Analyzer errors: {}'.format(results['analyzer_failure_count']))
    