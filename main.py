import marimo

__generated_with = "0.20.4"
app = marimo.App(width="full")


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
    import io, contextlib

    load_dotenv()
    from adapters import DeriveListner, SynthWorker

    return DeriveListner, SynthWorker, mo, os, pd, traceback


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
    deriv_listner.update_ui = lambda: None
    deriv_listner.start()
    return deriv_listner, forecast_fetch_interval, payouts, synth_worker


@app.cell
def _(mo):
    refresh = mo.ui.refresh(
        options=["1s", "2s", "5s", "10s"],
        default_interval="1s",
        label="Dashboard refresh interval",
    )
    refresh  # <-- renders the interval picker in the notebook
    return (refresh,)


@app.cell
def _():
    return


@app.cell
def _(deriv_listner, forecast_fetch_interval, payouts, refresh, synth_worker):
    # Touch `refresh` so marimo re-runs this cell on every tick.
    # The value itself is unused — the dependency is what matters.
    _ = refresh

    from dashboard_ui_cell import build_hft_dashboard

    build_hft_dashboard(
        payouts_df=payouts,
        synth_worker=synth_worker,
        deriv_listner=deriv_listner,
        forecast_fetch_interval=forecast_fetch_interval,
    )
    return


@app.cell
def _():
    # deriv_listner.stop()
    # synth_worker.stop_worker()
    return


@app.cell
def _(mo, synth_worker):
    interval_slider = mo.ui.slider(
        start=60,
        stop=1800,
        step=60,
        value=synth_worker.forecast_fetch_interval,
        label="Synth refresh interval (s)",
        show_value=True,
    )
    worker_switch = mo.ui.switch(value=True, label="DataListeners")

    mo.hstack([interval_slider, worker_switch], gap="2rem", align="center")


    # Cell 2: consume .value — only reads, never creates UI elements
    return interval_slider, worker_switch


@app.cell
def _(
    deriv_listner,
    interval_slider,
    mo,
    payouts,
    synth_worker,
    worker_switch,
):
    synth_worker.set_interval(interval_slider.value)

    mo.stop(
        not worker_switch.value
        and synth_worker.is_running is False
        and deriv_listner.is_running is False
    )

    if not worker_switch.value and (
        synth_worker.is_running or deriv_listner.is_running
    ):
        # Stop both workers
        synth_worker.stop_worker()
        deriv_listner.stop()
        # Clear stale option chain data so the table shows empty until fresh data arrives
        # payouts.drop(payouts.index, inplace=True)

    elif (
        worker_switch.value
        and not synth_worker.is_running
        and not deriv_listner.is_running
    ):
        # Clear before restart too — previous session rows have wrong expiry timestamps
        payouts.drop(payouts.index, inplace=True)
        deriv_listner.start()
    return


@app.cell
def _():
    return


@app.cell
def _(mo):
    repl_editor = mo.ui.code_editor(
        value="# Access any live variable: payouts, synth_worker, deriv_listner\n",
        language="python",
        min_height=120,
    )
    repl_run = mo.ui.run_button(label="Run", kind="success")

    mo.vstack(
        [
            mo.md("**Analyse or Shoot Orders**"),
            repl_editor,
            repl_run,
        ]
    )


    # Cell: REPL execution — only fires when button is clicked
    return repl_editor, repl_run


@app.cell
def _(
    deriv_listner,
    mo,
    payouts,
    repl_editor,
    repl_run,
    synth_worker,
    traceback,
):
    mo.stop(not repl_run.value)


    _ns = globals() | {
        "payouts": payouts,
        "synth_worker": synth_worker,
        "deriv_listner": deriv_listner,
        "mo": mo,
    }
    try:
        with mo.redirect_stdout():
            exec(repl_editor.value, _ns)
    except Exception:
        mo.output.append(
            mo.callout(mo.md(f"```\n{traceback.format_exc()}\n```"), kind="danger")
        )
    return


if __name__ == "__main__":
    app.run()
