import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
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
    from dotenv import load_dotenv

    load_dotenv()
    from adapters import DeriveListner, SynthWorker

    return DeriveListner, SynthWorker, os, pd


@app.cell
def _(os):
    deriv_api_key, deriv_acct_id, deriv_app_id, synth_api_key = (
        os.getenv("DERIVE_API_KEY"),
        os.getenv("DERIVE_DEMO_ACCT_ID"),
        os.getenv("DERIVE_APP_ID"),
        os.getenv("SYNTH_API_KEY"),
    )
    return deriv_acct_id, deriv_api_key, deriv_app_id, synth_api_key


@app.cell
def _(
    DeriveListner,
    SynthWorker,
    deriv_acct_id,
    deriv_api_key,
    deriv_app_id,
    pd,
    synth_api_key,
):
    fair_payouts = pd.DataFrame(
        columns=[
            "index",
            "0.005",
            "0.05",
            "0.2",
            "0.35",
            "0.5",
            "0.65",
            "0.8",
            "0.95",
            "0.995",
            "expiration_ts",
            "c_fair_payout",
            "p_fair_payout",
        ]
    )
    payouts = pd.DataFrame(
        columns=[
            "uly_symbol",
            "CE-payout",
            "PE-payout",
            "CE_spot_ts",
            "PE_spot_ts",
            "c_fair_payout",
            "p_fair_payout",
            "p_edge_pct",
            "c_edge_pct",
        ]
    )
    payouts.index.name = "contract"

    forecasted_expires_ts = []
    forecast_fetch_interval = 10 * 60
    params = {
        "asset": "XAU",
        "horizon": "1h",
        "days": 10,
        "limit": 5,
    }

    synth_worker = SynthWorker(
        api_key=synth_api_key,
        fair_payouts=fair_payouts,
        forecast_fetch_interval=forecast_fetch_interval,
        forecasted_expires_ts=forecasted_expires_ts,
        params=params,
    )

    deriv_listner = DeriveListner(
        acct_id=deriv_acct_id,
        api_key=deriv_api_key,
        app_id=deriv_app_id,
        expirations_ts=forecasted_expires_ts,
        fair_payouts=fair_payouts,
        payouts=payouts,
        synth_worker=synth_worker,
    )
    deriv_listner.start()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
