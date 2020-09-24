from src.scanner.equity import EquityScanner
from src.scanner.option import OptionScanner
from src.analyzer.regression import RegressionAnalyzer
from src.analyzer.cps import CreditPutSpreadAnalyzer
import os


if __name__ == '__main__':

    analyzer = RegressionAnalyzer()
    scanner = EquityScanner(
        uni_file='universe/active-equities.csv',
        analyzer=analyzer,
        num_processes=6
    )
    results = scanner.run()
    print(results)

    # analyzer = CreditPutSpreadAnalyzer()
    # scanner = OptionScanner(
    #     uni_file='universe/test-symbols.csv', 
    #     analyzer=analyzer,
    #     num_processes=6
    # )

    # results = scanner.run()
    # print('Fetching errors: {}'.format(results['fetch_failure_count']))
    # print('Analyzer errors: {}'.format(results['analyzer_failure_count']))

    # TODO:
    #   1. [DONE] add option to scanners to load list of symbols instead of file of symbols
    #   2. add same two features to cps rank engine
    #   3. [DONE] bolster equity scanner (simple route with two regressions) logic
    #   4. [DONE] add feature to equity scanner to pull symbol correlation with market index
    #   5. [DONE] updgrade equity scanner to save scan results
    #   6. [DONE] add logging to equity scanner
    
    