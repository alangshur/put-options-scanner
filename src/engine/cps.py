from src.engine.base import EngineBase
import csv


class CreditPutSpreadRankEngine(EngineBase):

    def __init__(self,
        equity_scan_file=None,
        equity_scan_list=None,
        option_scan_file=None,
        option_scan_list=None
    ):

        self.equity_scan_file=None
        self.equity_scan_list=None
        self.option_scan_file=None
        self.option_scan_list=None

    def run(self):
        pass

    def __load_csv_file(self, file_name):

        # read csv file
        f = open(file_name, 'r')
        uni_list = list(csv.reader(f))
        data = [row[0] for row in uni_list[0:]]        
        f.close()
        
        return data