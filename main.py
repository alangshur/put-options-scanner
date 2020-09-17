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

    analyzer = CreditPutSpreadAnalyzer(
        risk_free_rate=0.68
    )
    scanner = OptionScanner(
        uni_file='universe/active-equities.csv', 
        analyzer=analyzer,
        num_processes=os.cpu_count()
    )
    results = scanner.run()
    spreads = sum(results['results'].values(), [])
    print('\n\n\nSpreads:\n')
    for spread in spreads:
        print(spread)
