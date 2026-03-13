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
    import websocket
    import ssl
    import threading
    import numpy as np
    from datetime import datetime
    import pandas as pd

    return datetime, httpx, json, mo, np, pd, pl, ssl, websocket


@app.cell
def _(json):
    with open("data/synth.json") as f:
        raw = json.load(f)

    api_key, acct_id, app_id = (
        raw["deriv_api_key"],
        raw["deriv_demo_acct_id"],
        raw["deriv_app_id"],
    )
    return acct_id, api_key, app_id


@app.cell
def _():
    from zoneinfo import ZoneInfo
    from time import sleep

    return ZoneInfo, sleep


@app.cell
def _(
    ZoneInfo,
    acct_id,
    api_key,
    app_id,
    datetime,
    httpx,
    json,
    mo,
    np,
    pd,
    sleep,
    ssl,
    websocket,
):
    EXPIRY_COUNT = 25
    EXPIRY_INTERVAL = 300
    start_ts = int(datetime.now().timestamp())
    start_ts = (start_ts // EXPIRY_INTERVAL) * EXPIRY_INTERVAL  # Round to 5min
    epochs = np.arange(
        start_ts, start_ts + EXPIRY_COUNT * EXPIRY_INTERVAL, EXPIRY_INTERVAL
    )
    get_ws, set_ws = mo.state(None)
    px_req_msg = [
        {
            "proposal": 1,
            "amount": 10,
            "basis": "stake",
            "contract_type": "CALL",
            "date_expiry": int(ts),
            "underlying_symbol": "frxXAUUSD",
            "subscribe": 1,
            "currency": "USD",
        }
        for ts in epochs
    ] + [
        {
            "proposal": 1,
            "amount": 10,
            "basis": "stake",
            "contract_type": "PUT",
            "date_expiry": int(ts),
            "underlying_symbol": "frxXAUUSD",
            "subscribe": 1,
            "currency": "USD",
        }
        for ts in epochs
    ]

    px_req_msg = sorted(
        px_req_msg, key=lambda x: f"{x.get('date_expiry')}{x.get('contract_type')}"
    )

    responses = []
    req_messages = [
        {"active_symbols": "full"},
        {"contracts_for": "frxXAUUSD"},
        {"contracts_list": 1},
        *px_req_msg,
    ]

    # Initialize DataFrame properly
    payouts = pd.DataFrame(
        columns=["CE-payout", "PE-payout", "CE_spot_ts", "PE_spot_ts"]
    )
    payouts.index.name = "contract"
    sender_thread = None


    def on_open(ws):
        # print("✓ WebSocket connected")
        # for msg in req_messages:
        #     ws.send(json.dumps(msg))
        #     sleep(0.1)
        def run(*args):
            print("✓ WebSocket connected - Sending throttled requests...")
            for i, msg in enumerate(req_messages):
                try:
                    ws.send(json.dumps(msg))
                    # Throttle: 10 requests per second
                    sleep(1)

                    if i % 10 == 0:
                        print(f"Sent {i}/{len(req_messages)} requests...")
                except Exception as e:
                    print(f"Send error: {e}")
                    break
            print("Done sending initial requests.")

        # Run sending in a separate thread so it doesn't block the listener
        # sender_thread = threading.Thread(target=run).start()
        sender_thread = mo.Thread(target=run).start()


    def to_readable(ts, with_date=True):
        dt_time_fmt = "%Y-%m-%d %I:%M:%S %p %Z" if with_date else "%I:%M:%S"
        ts_tz = datetime.fromtimestamp(ts, tz=ZoneInfo("Asia/Kolkata"))

        # Format into a human-readable string
        readable_time = ts_tz.strftime(dt_time_fmt)
        return readable_time


    def on_message(ws, message):
        try:
            responses.append(message)
            msg = json.loads(message)
            # print(msg)

            if msg.get("msg_type") == "proposal":
                if msg.get("error") is not None:
                    return
                # FIXED: Correctly access the 'contract_type' key instead of 'CALL'
                req = msg["echo_req"]
                readable_exp_time = to_readable(req["date_expiry"])
                contract = f"{req['underlying_symbol']}-{readable_exp_time}"

                payout_column = (
                    "CE-payout" if req["contract_type"] == "CALL" else "PE-payout"
                )

                readable_spot_time = to_readable(
                    msg["proposal"]["spot_time"], with_date=False
                )
                spot_ts_column = (
                    "CE_spot_ts"
                    if req["contract_type"] == "CALL"
                    else "PE_spot_ts"
                )

                payout = msg["proposal"]["payout"]

                # FIXED: Use .loc to properly update or append rows in Pandas
                payouts.loc[contract, payout_column] = payout
                payouts.loc[contract, spot_ts_column] = readable_spot_time

                # FIXED: Use mo.output.replace for a flicker-free streaming UI in Marimo
                mo.output.replace(
                    mo.ui.table(
                        payouts.reset_index(),  # Reset index so 'contract' shows as a column
                        selection="multi",
                        pagination=True,
                        page_size=50,
                    )
                )
        except Exception as e:
            print(f"Excption  {e} {msg}")


    def on_error(ws, message):
        print(f"Error: {message}")


    def on_close(ws, close_status_code, close_msg):
        print("\n🔌 Connection closed")


    # API Connection setup
    base_url = "https://api.derivws.com"
    otp_url = f"{base_url}/trading/v1/options/accounts/{acct_id}/otp"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Deriv-App-ID": app_id,
    }

    # Fetch WebSocket URL via HTTPX
    res = httpx.post(otp_url, headers=headers)
    ws_url = res.json().get("data", {}).get("url")

    ws_thread = None
    ws = None


    def start_websocket():
        # Important: Reference the current thread so we can close it gracefully
        ws_thread = mo.current_thread()

        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        set_ws(ws)

        # Run this in a loop that checks if the Marimo cell has been re-run/deleted
        # ws.run_forever is a blocking call, but now it's inside the Thread target
        ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})


    mo.Thread(target=start_websocket).start()
    return get_ws, responses


@app.cell
def _(get_ws):
    get_ws().close()
    return


@app.cell
def _(json, responses):
    json.loads(responses[-1])
    return


@app.cell
def _(json, pl, responses):
    pl.from_dicts(json.loads(responses[3])["contracts_for"]["available"])
    return


@app.cell
def _(json, pl, responses):
    pl.from_dicts(json.loads(responses[1])["contracts_list"])
    return


if __name__ == "__main__":
    app.run()
