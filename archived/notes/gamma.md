# Gamma Scalping Strategy

## Methodology

Find stocks that have much higher historical volatility percentile than implied volatility percentile. Then buy straddle or strangle (measured easily using minimum internal range) and actively gamma scalp to collect daily profit greater than time decay. Execute study to determine the best time period ATR for a specific asset (maybe the time period ATR is dynamic and determined on-the-fly) in order to update positions:

- High-frequency update: small updates to capture many oscillations in a liquid option if ATR is small and the variance (oscillations) is high on a short time-scale.

- Low-frequery update: large updates on a daily+ time-scale if option is less liquid (must traverse bid-ask spread) and it is trending in a less variant manner.
