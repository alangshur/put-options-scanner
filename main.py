from src.tools.scanner import StockScanner

scanner = StockScanner('universe/active-equities.csv')
scanner.run()