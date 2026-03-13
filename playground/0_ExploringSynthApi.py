import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import hvplot.polars
    import httpx
    import polars as pl
    import os
    import json

    return httpx, json


@app.cell
def _(json):
    with open("data/synth.json") as f:
        raw = json.load(f)

    api_key = raw["synth_api_key"]
    return (api_key,)


@app.cell
def _(api_key, httpx):
    base_url = "https://api.synthdata.co/"
    pctile_url = f"{base_url}insights/prediction-percentiles"
    params = {
        "asset": "XAU",
        "horizon": "1h",
        "days": 10,
        "limit": 5,
    }  # start time in ISO8601 fmt
    headers = {"Authorization": f"Apikey {api_key}"}

    # curl "https://api.synthdata.co/v2/prediction/best?asset=BTC&time_increment=300&time_length=86400" \
    # -H "Authorization: Apikey YOUR_API_KEY"
    response = httpx.get(pctile_url, headers=headers, params=params)
    return (response,)


@app.cell
def _(response):
    response.json()
    return


@app.function
def price_binary_option(
    percentile_step,
    strike,
    payoff=1.0,
    discount_factor=1.0,
    option_type="call",
):
    """
    Prices a Cash-or-Nothing binary option using linear interpolation
    of the provided percentile distribution.

    :param percentile_step: Dict from the API (e.g., {"0.05": 88000, "0.5": 89000...})
    :param strike: The strike price (K)
    :param payoff: The fixed payout amount (default $1)
    :param discount_factor: e^(-rt), defaults to 1.0 for simple probability
    :param option_type: "call" (pays if S > K) or "put" (pays if S < K)
    """
    # 1. Extract and sort the percentiles and their corresponding prices
    # We convert keys to floats and values to floats
    sorted_data = sorted(
        [(float(k), float(v)) for k, v in percentile_step.items()],
        key=lambda x: x[1],
    )

    percentiles = [p[0] for p in sorted_data]
    prices = [p[1] for p in sorted_data]

    # 2. Handle cases outside the known distribution bounds
    if strike <= prices[0]:
        prob_below = 0.0
    elif strike >= prices[-1]:
        prob_below = 1.0
    else:
        # 3. Linear Interpolation to find the percentile (CDF) for the strike
        import bisect

        idx = bisect.bisect_right(prices, strike)

        # Prices between which the strike falls
        x0, x1 = prices[idx - 1], prices[idx]
        # Percentiles between which the strike falls
        y0, y1 = percentiles[idx - 1], percentiles[idx]

        # Linear interpolation formula: y = y0 + (x - x0) * (y1 - y0) / (x1 - x0)
        prob_below = y0 + (strike - x0) * (y1 - y0) / (x1 - x0)

    # 4. Calculate Option Value
    if option_type.lower() == "call":
        event_probability = 1.0 - prob_below
    else:
        event_probability = prob_below

    return event_probability * payoff * discount_factor


@app.cell
def _():
    # --- Example Usage ---
    # Data from the 5th minute step of a 1h horizon
    step_data = {
        "0.005": 88631.35,
        "0.05": 88765.15,
        "0.2": 88879.70,
        "0.35": 88943.39,
        "0.5": 88998.23,
        "0.65": 89055.24,
        "0.8": 89122.30,
        "0.95": 89236.58,
        "0.995": 89359.79,
    }

    strike_price = 89100
    fair_value = price_binary_option(step_data, strike_price)

    print(f"Fair Price of Binary Call at strike {strike_price}: ${fair_value:.4f}")
    return


@app.cell
def _(response):
    (
        response.json().get("forecast_start_time"),
        response.json().get("forecast_future").get("percentiles")[-1],
    )
    return


if __name__ == "__main__":
    app.run()
