from src.executor.portfolio import PortfolioExecutor
import csv


if __name__ == '__main__':
    
    # execute portfolio read
    executor = PortfolioExecutor()
    executor.run_portfolio_read()