import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    os.environ["MARIMO_NO multiprocessing"] = "1"  
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
    from zoneinfo import ZoneInfo
    from time import sleep
    from threading import Event
    import polars.selectors as cs
    import json
    import traceback

    return (
        Event,
        ZoneInfo,
        cs,
        datetime,
        httpx,
        json,
        mo,
        os,
        pd,
        pl,
        sleep,
        ssl,
        traceback,
        websocket,
    )


@app.cell
def _(json):
    with open("data/synth.json") as f:
        raw = json.load(f)

    deriv_api_key, deriv_acct_id, deriv_app_id, synth_api_key = (
        raw["deriv_api_key"],
        raw["deriv_demo_acct_id"],
        raw["deriv_app_id"],
        raw["synth_api_key"],
    )
    return deriv_acct_id, deriv_api_key, deriv_app_id, synth_api_key


@app.cell
def _(
    Event,
    ZoneInfo,
    cs,
    datetime,
    deriv_acct_id,
    deriv_api_key,
    deriv_app_id,
    httpx,
    json,
    mo,
    os,
    pd,
    pl,
    sleep,
    ssl,
    synth_api_key,
    traceback,
    websocket,
):
    import bisect


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


    class SynthWorker:
        def __init__(
            self,
            api_key,
            fair_payouts: pd.DataFrame,
            params: dict,
            forecasted_expires_ts: list,
            forecast_fetch_interval: int,
        ):
            self.fair_payouts = fair_payouts
            self.api_key = api_key
            self.params = params
            self.price_distributions = None
            self.forecasted_expires_ts = forecasted_expires_ts
            self.fair_payout_with_latest_spot = None
            self.stop_evt = Event()
            self.forecast_fetch_interval = forecast_fetch_interval
            self.spot = None
            self.default_stake = 10
            self.cache_dir = "data/api_response/synth/"
            os.makedirs(self.cache_dir, exist_ok=True)

        def fetch_synth_forecast(self):
            base_url = "https://api.synthdata.co/"
            pctile_url = f"{base_url}insights/prediction-percentiles"
            headers = {"Authorization": f"Apikey {self.api_key}"}

            response = httpx.get(
                pctile_url, headers=headers, params=self.params, timeout=300
            )
            response.raise_for_status()
            res = response.json()
            cache_file = f"data/api_response/synth/{datetime.now().strftime('%Y%m%d_%I%M%S')}.json"

            with open(
                cache_file,
                "w",
            ) as f:
                json.dump(res, f)

            forecast_start_time = res.get("forecast_start_time")
            self.current_spot = res.get("current_price")
            pctile_raw = res.get("forecast_future").get("percentiles")

            forecast_start_ts = int(
                datetime.fromisoformat(forecast_start_time).timestamp()
            )

            horizon = params["horizon"]
            if horizon == "1h":
                interval = 60
            else:
                interval = 5 * 60

            default_stake = 10
            pctile = (
                pl.from_dicts(pctile_raw)
                .with_row_index()
                .with_columns(
                    (forecast_start_ts + pl.col("index") * pl.lit(interval)).alias(
                        "expiration_ts"
                    ),
                )
            )
            # TOCHECK: Some delta based approach, so we're not replacing the whole thing every time we fetch response
            self.forecasted_expires_ts = pctile.get_column(
                "expiration_ts"
            ).to_list()
            self.price_distributions = pctile
            return pctile, self.current_spot

        def calculate_fair_payouts(self, spot_px):
            pctile_vals = self.price_distributions.select(
                cs.exclude("index", "expiration_ts")
            ).columns
            pctile = self.price_distributions
            fair_payouts_w_dist = (
                pctile.slice(
                    1,
                )
                .with_columns(
                    pctile.slice(
                        1,
                    )
                    .drop("index", "expiration_ts")
                    .map_rows(
                        lambda prices: Pricer.wrapper(
                            px_dist=prices,
                            pctile_vals=pctile_vals,
                            strike_price=spot_px,
                            option_type="call",
                            stake=self.default_stake,
                        )
                    )
                    .get_column("map")
                    .alias("c_fair_payout"),
                    pctile.slice(
                        1,
                    )
                    .drop("index", "expiration_ts")
                    .map_rows(
                        lambda prices: Pricer.wrapper(
                            px_dist=prices,
                            pctile_vals=pctile_vals,
                            strike_price=spot_px,
                            option_type="put",
                            stake=self.default_stake,
                        )
                    )
                    .get_column("map")
                    .alias("p_fair_payout"),
                )
                .with_columns(
                    pl.from_epoch(pl.col("expiration_ts"), time_unit="s")
                    .dt.replace_time_zone("UTC")
                    .dt.convert_time_zone("Asia/Kolkata")
                    .dt.strftime("%Y-%m-%d %I:%M:%S %p %Z")
                    .alias("readable_ts")
                )
            )
            self.fair_payouts = fair_payouts_w_dist.to_pandas().set_index(
                "readable_ts"
            )
            return self.fair_payouts

        def stop_worker(self):
            self.stop_evt.set()
            pass

        def update_ui_data(self, msg=None):
            if msg:
                mo.output.replace(
                    mo.vstack(
                        [
                            mo.md(msg),
                            mo.ui.table(
                                self.fair_payouts,
                                selection="multi",
                                pagination=True,
                                page_size=50,
                                show_column_summaries=False,
                                label="DerivDen",
                            ),
                        ]
                    )
                )
            else:
                mo.output.replace(
                    mo.vstack(
                        [
                            mo.ui.table(
                                self.fair_payouts,
                                selection="multi",
                                pagination=True,
                                page_size=50,
                                show_column_summaries=False,
                                label="DerivDen",
                            ),
                        ]
                    )
                )

        def update_spot(self, spot_px):
            fair_payouts = self.calculate_fair_payouts(spot_px=spot_px)
            # self.update_ui_data()

        def start_worker(self):
            self.stop_evt.clear()
            try:
                self.is_running = True
                try:
                    self.ctx = mo.current_thread()
                except:
                    pass  # worker wasn't started in marimo context
                while not self.stop_evt.is_set():
                    mo.output.replace(mo.md("Fetching Data From Synth"))
                    price_distributions, current_spot = self.fetch_synth_forecast()
                    fair_payouts = self.calculate_fair_payouts(
                        spot_px=current_spot
                    )
                    # stop_fetching_forecast.wait(timeout=self.forecast_fetch_interval)

                    for remaining in range(self.forecast_fetch_interval, 0, -1):
                        if self.stop_evt.is_set():
                            break

                        # Update the UI with the existing table and the new countdown
                        # self.update_ui_data(
                        #     f"**Next data refresh in:** `{remaining}s`"
                        # )

                        sleep(1)
            except Exception as e:
                err_stack = traceback.format_exc()
                print(f"ERROR : {e} \n {err_stack}")
            finally:
                self.is_running = False

        def start(self):
            mo.Thread(target=self.start_worker).start()


    from collections import deque


    class DeriveListner:
        def __init__(
            self,
            fair_payouts: pd.DataFrame,
            payouts: pd.DataFrame,
            acct_id,
            api_key,
            app_id,
            expirations_ts,
            synth_worker: SynthWorker,
        ):
            self.fair_payouts = fair_payouts
            self.account_id = acct_id
            self.app_id = app_id
            self.api_key = api_key
            self.expirations_ts = expirations_ts
            self.ws = None
            self.ws_url = None
            self.init_payload = []
            self.synth_worker: SynthWorker = synth_worker
            self.payouts = payouts
            self.ws_msg_history = deque(maxlen=100)
            self.columns_to_be_styled = ["c_fair_payout", "p_fair_payout"]
            self.reference_col = {
                "c_fair_payout": "CE-payout",
                "p_fair_payout": "PE-payout",
            }
            self.display_col_seq = "contract,CE-payout,c_fair_payout,CE_spot_ts,PE-payout,p_fair_payout,PE_spot_ts".split(
                ","
            )
            self.uly_symbol = "frxXAUUSD"

        def get_ws_url(self):
            base_url = "https://api.derivws.com"
            otp_url = (
                f"{base_url}/trading/v1/options/accounts/{self.account_id}/otp"
            )
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Deriv-App-ID": self.app_id,
            }

            # Fetch WebSocket URL via HTTPX
            res = httpx.post(otp_url, headers=headers)
            self.ws_url = res.json().get("data", {}).get("url")
            return self.ws_url

        def to_readable(self, ts, with_date=True):
            dt_time_fmt = "%Y-%m-%d %I:%M:%S %p %Z" if with_date else "%I:%M:%S"
            ts_tz = datetime.fromtimestamp(ts, tz=ZoneInfo("Asia/Kolkata"))

            # Format into a human-readable string
            readable_time = ts_tz.strftime(dt_time_fmt)
            return readable_time

        def prepare_subscription_payload(self):
            if len(self.synth_worker.forecasted_expires_ts) == 0:
                synth_res = self.synth_worker.fetch_synth_forecast()

            epochs = fair_payouts["expiration_ts"].tolist()
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
                for ts in self.synth_worker.forecasted_expires_ts
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
                for ts in self.synth_worker.forecasted_expires_ts
            ]

            px_req_msg = sorted(
                px_req_msg,
                key=lambda x: f"{x.get('date_expiry')}{x.get('contract_type')}",
            )

            self.init_payload = [
                # {"active_symbols": "full"},
                # {"contracts_for": "frxXAUUSD"},
                # {"contracts_list": 1},
                *px_req_msg,
            ]

        def _on_open(self, ws):
            def run():
                print("✓ WebSocket connected - Sending throttled requests...")
                for i, msg in enumerate(self.init_payload):
                    try:
                        self.ws.send(json.dumps(msg))
                        sleep(0.1)  # Throttle
                        if i % 10 == 0:
                            print(f"Sent {i}/{len(self.init_payload)} requests...")
                    except Exception as e:
                        print(f"Send error: {e}")
                        break

            mo.Thread(target=run).start()

        def update_ui(self):
            def style_cell(_rowId, _columnName, value):
                if _columnName not in self.columns_to_be_styled:
                    return {}

                mkt_payout_col = self.reference_col.get(_columnName)
                mkt_payout = deriv_listner.payouts.iloc[int(_rowId)][
                    mkt_payout_col
                ]

                try:
                    # Percentage difference for relative scaling
                    pct_diff = (mkt_payout - value) / value if value != 0 else 0
                    max_pct = 0.5  # 50% difference = max intensity

                    intensity = min(abs(pct_diff) / max_pct, 1.0)

                    if pct_diff > 0:
                        # Green: hsl(120, saturation%, 50%)
                        sat = int(30 + 70 * intensity)
                        bg = f"hsl(120, {sat}%, {50 - 20 * intensity}%)"
                    else:
                        # Red: hsl(0, saturation%, 50%)
                        sat = int(30 + 70 * intensity)
                        bg = f"hsl(0, {sat}%, {50 - 20 * intensity}%)"
                except:
                    return {}

                return {
                    "backgroundColor": bg,
                    "color": "white" if intensity > 0.4 else "black",
                    "fontWeight": "bold",
                }

            mo.output.replace(
                mo.ui.table(
                    self.payouts.reset_index()[self.display_col_seq],
                    pagination=True,
                    page_size=20,
                    style_cell=style_cell,
                    show_column_summaries=False,
                    show_download=False,
                    show_data_types=False,
                    selection="multi-cell",
                    label=f"{self.uly_symbol} Mkt Payout Vs Fair Payout For Rise/Fall Binary Options at current spot",
                )
            )

        def _on_message(self, ws, message):
            try:
                self.ws_msg_history.append(message)
                msg = json.loads(message)
                if msg.get("msg_type") == "proposal":
                    if msg.get("error"):
                        return

                    req = msg["echo_req"]
                    expiry_str = self.to_readable(req["date_expiry"])
                    spot_time_str = self.to_readable(
                        msg["proposal"]["spot_time"], with_date=False
                    )
                    spot_px = msg["proposal"]["spot"]

                    self.synth_worker.update_spot(spot_px)

                    # Determine columns based on type
                    is_call = req["contract_type"] == "CALL"
                    p_col = "CE-payout" if is_call else "PE-payout"
                    s_col = "CE_spot_ts" if is_call else "PE_spot_ts"
                    f_col = "c_fair_payout" if is_call else "p_fair_payout"

                    # Update Local Dataframe
                    self.payouts.loc[expiry_str, p_col] = msg["proposal"]["payout"]
                    self.payouts.loc[expiry_str, s_col] = spot_time_str
                    self.payouts.loc[expiry_str, "uly_symbol"] = req[
                        "underlying_symbol"
                    ]

                    # Lookup fair payout from the SynthWorker data
                    if expiry_str in self.synth_worker.fair_payouts.index:
                        self.payouts.loc[expiry_str, f_col] = round(
                            float(
                                self.synth_worker.fair_payouts.loc[
                                    expiry_str, f_col
                                ]
                            ),
                            2,
                        )

                    # Update UI
                    self.update_ui()

            except Exception as e:
                err_stack = traceback.format_exc()
                print(f"Message Processing Error: {e} \n {err_stack}")

        def _on_error(self, ws, error):
            print(f"WS Error: {error}")

        def _on_close(self, ws, close_status_code, close_msg):
            print("🔌 Connection closed")
            self.is_running = False

        def stop(self):
            if self.ws:
                self.ws.close()
            self.is_running = False

        def start(self):
            def run_ws():
                try:
                    self.is_running = True
                    self.ws = websocket.WebSocketApp(
                        self.ws_url,
                        on_open=self._on_open,
                        on_message=self._on_message,
                        on_error=self._on_error,
                        on_close=self._on_close,
                    )
                    self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
                except Exception as e:
                    err_stack = traceback.format_exc()
                    print(f"ERROR : {e} \n {err_stack}")
                finally:
                    self.is_running = False

            self.prepare_subscription_payload()
            print("Prepared init payload")
            self.get_ws_url()
            print("Got ws url")
            # Start the WebSocket in a background thread
            mo.Thread(target=run_ws).start()
            self.synth_worker.start()


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
    # synth_worker.stop_worker()
    # deriv_listner.stop()
    # # del synth_worker
    # # del deriv_listner
    return


if __name__ == "__main__":
    app.run()
