# Wheel Strategy

## Picking Positions

1. Run scanner
2. Pick positions with best ROC/moneyness
3. Verify underlying has traded in healthy price range with decent upward momentum
4. Verify underlying has buy rating (CNN Business Forecast) and long-term increase
5. Set limit above ask

## Managing Positions

- Close for a loss
  - Situation 1: If underlying/market tanks without signs of possible rebound, close for a loss early to protect position.
- Close for a profit
  - Situation 1: If underlying moves early, close at a profit (set a stop loss above current price).
  - Situation 2: If in the green and the underlying starts reversing, close at a profit (set a stop loss above current price).
- Roll for a credit:
  - Situation 1: If underlying dips, roll down for a net credit if possible to the safest delta that still ensures a debit. It is better to be proactive about this move, or rolling down for a credit will be super difficult.
- Roll for a debit:
  - Situation 1: Only roll for a debit if it will provide necessary protection against downwards movement in the underlying (and is less than closing position).
- Leg into spread:
  - Situation 1: If there is significant downards movement that may be temporary, we can leg into a spread if it avoids a net debit to protect the downside.
- Take assignment from put:
  - Situation 1: If confident in the movement of the underlying below the put strike, take assignment and begin selling calls.
- Move call down:
  - Situation 1: If underlying is showing unwillingness to move back up or has dipped enough, move call down to collect greater premium or accept a loss (can move down to cost basis to take zero loss).
- Move call up:
  - Situation 1: If underlying/market is trending towards call, move call up to take larger profit.

## Automation

- Area 1: Partial automation to scan for the best underlying indices. The actual positions and sizes themselves will be placed manually (sized as a fixed fraction of buying power).
- Area 2: Monitoring the portfolio:
  - Daily P/L based on theta decay
  - Cost basis from premium collection
- Area 3: Updating portfolio positions:
  - Continuous scanning to roll up/down
  - Continuous scanning to close positions
  - Continuous scanning to leg into spread
  - Addition of new calls once expired
  - Daily stop loss set at the beginning of the day