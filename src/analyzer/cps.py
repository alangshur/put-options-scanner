from src.analyzer.base import OptionAnalyzerBase
from datetime import datetime, date

class CreditPutSpreadAnalyzer(OptionAnalyzerBase):

    def __init__(self):
        super().__init__('credit_put_spread_analyzer')

    def run(self, 
        symbol, 
        quote,
        expiration, 
        chain
    ):

        return 0.0

    def validate(self, 
        symbol=None, 
        quote=None,
        expiration=None, 
        chain=None
    ):
        
        # filter expirations
        if expiration is not None:

            # get dte
            now_dt = date.today()
            exp_dt = datetime.strptime(expiration, '%Y-%m-%d').date()
            dte = (exp_dt - now_dt).days

            # target dte range
            if dte < 21 or dte > 91: return False
            else: return True

        else: return True