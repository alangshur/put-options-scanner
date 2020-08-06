# Plan

Pipeline:
    - Read current pipeline config file (stores universe, last update datetime, etc)
    - Load last year of daily returns for universe assets
        - Calculate SPY correlation for each
        - Calculate trend direction (positive/neutral return over days/weeks/months)
        - Calculate RSI, BB, MACD (simple filtering from research)
        - Filter out assets with bad trend/technicals
    - Fetch earnings (python library) for filtered universe:
        - Filter out assets with earnings before expiration (research: how much to avoid expirations?)
    - Run option script on filtered universe
        - Narrow down best spread per symbol
        - Build beta-weighted net delta portfolios

Position management (ordered options):
    1. Trailing stop once 50% profit
    2. Rolling position forwards and down:
        - If net credit for buying bad PS and selling lower PS
    3. Legging into IC or IB:
        - If underlying moves against PS, sell IC/IB with lower max loss than premium from PS to lower cost basis
        - Modify widtth of IC/IB (broken wing) to ensure net profit if underlying reverts
    4. Close at short strike for smaller loss

Improvements:
    - Monitor the same positions throughout the day (store in hashtable)
    - Filter out earnings/underlying movement (returns)/volatility cycles (forecasting IV)
    - Plot chart in value of a current spread to catch momentum
    - Alert to email/phone number with exception positions
    - Build rr_percent and iv_z_score and short_delta (adj_profit) into single score
    - Continuously monitor throughout day to find highest scoring positions in market

Main philosophy:
    - Defined risked plays with near-or-greater-than 1:1 risk-return ratios. With OTM short positions (0.5-1.0 STD) and no significant downard pressure/events, the odds are simply in our favor — just need to be methodical which is the hardest part.
