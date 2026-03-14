# Quickstart 

1. Copy .example.env to .env and configure the details
2. ``` uv sync ```
3. ``` uv run marimo run main.py```
This will open a browser window where you can see it running and showing fair payouts and edge after receiving data from deriv and synth.

# Architecture Overview  
Scanner is a marimo app that streams live data. It integrates two data sources and responds to two update events to maintain a master dataframe. DerivDen checks for contracts expiring within one hour, as Synth provides minute‑level data for the 1‑hour horizon.

### Data Update Events  
1. **SynthData API call** – Polled at a fixed refresh interval.  
2. **Deriv WebSocket messages** – Stream spot prices and updated quotes for active contracts.

# How Synth API is Integrated  
For this proof of concept, we subscribe to binary rise/fall options on Deriv’s WebSocket for expirations covered by Synth’s `insights/prediction‑percentiles` endpoint. A linear interpolation of the percentile‑price pairs gives the full probability distribution, which is then used to price each option. The fair payout is calculated as:  
`fair_payout = probability × event × discount_factor` (discount_factor = 1 for simplicity).

# Data Consumption Approach  
Two background workers run concurrently:  
- One polls the Synth REST API at regular intervals.  
- The other is a WebSocket client that continuously aggregates incoming data into a shared DataFrame and recomputes option prices on every tick.

# Key Insights  
The house almost always holds an extraordinary edge in Gold (XAU) binary options on Deriv, especially for near‑term expiries. Put–call parity shows that a paired rise/fall contract typically costs 10–15% less than its maximum payout. For example, buying a pair at $20 yields at most $18 in total payout – a 15% edge for the house. Further investigation is needed to see whether Synth’s probabilistic forecasts could tip the odds back in the trader’s favor.