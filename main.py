from src.scanner.equity import EquityScanner
from src.scanner.option import OptionScanner
from src.analyzer.regression import RegressionAnalyzer
from src.analyzer.cps import CreditPutSpreadAnalyzer


# scanner = EquityScanner(
#     uni_file='universe/active-equities.csv',
#     analyzer=RegressionAnalyzer(),
#     num_threads=10
# )

scanner = OptionScanner(
    uni_file='universe/active-equities.csv', 
    analyzer=CreditPutSpreadAnalyzer(),
    num_threads=20
)

results = scanner.run()
print(results)