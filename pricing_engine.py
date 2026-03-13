import bisect
import json
import os
import ssl
import traceback
from collections import deque
from datetime import datetime
from threading import Event, Lock, Thread
from zoneinfo import ZoneInfo

import httpx
import marimo as mo
import pandas as pd
import polars as pl
import polars.selectors as cs
import websocket
import json
from time import sleep


class Pricer:
    @staticmethod
    def price_binary_option(
        percentile_step,
        strike,
        payoff=None,
        stake=None,
        discount_factor=1.0,
        option_type="call",
    ):
        """
        Prices a Cash-or-Nothing binary option using linear interpolation.
        Access via: Pricer.price_binary_option(...)
        """
        assert payoff is not None or stake is not None, (
            "Either payoff or stake is required"
        )

        # 1. Extract and sort the percentiles and their corresponding prices
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
            # 3. Linear Interpolation
            idx = bisect.bisect_right(prices, strike)
            x0, x1 = prices[idx - 1], prices[idx]
            y0, y1 = percentiles[idx - 1], percentiles[idx]

            # Linear interpolation formula
            prob_below = y0 + (strike - x0) * (y1 - y0) / (x1 - x0)

        # 4. Calculate Option Value
        if option_type.lower() == "call":
            event_probability = 1.0 - prob_below
        else:
            event_probability = prob_below

        # Avoid division by zero if probability is 0
        if event_probability <= 0:
            return 0.0 if payoff else float("inf")

        if payoff is None:
            # Returns fair payoff for a given stake
            return stake / (event_probability * discount_factor)
        else:
            # Returns fair stake (price) for a given payoff
            return event_probability * payoff * discount_factor

    @staticmethod
    def wrapper(px_dist, pctile_vals, strike_price, stake, option_type="call"):
        """
        Helper to format raw lists into the dictionary expected by price_binary_option.
        """
        assert len(px_dist) == len(pctile_vals), "invalid distribution shape"

        # Map prices to percentiles
        dist = {str(pct): px for pct, px in zip(pctile_vals, px_dist)}

        return Pricer.price_binary_option(
            dist, strike_price, stake=stake, option_type=option_type
        )
