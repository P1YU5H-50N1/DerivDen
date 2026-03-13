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
from time import sleep
from pricing_engine import Pricer


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
        cache_file = (
            f"data/api_response/synth/{datetime.now().strftime('%Y%m%d_%I%M%S')}.json"
        )

        with open(
            cache_file,
            "w",
        ) as f:
            json.dump(res, f)

        forecast_start_time = res.get("forecast_start_time")
        self.current_spot = res.get("current_price")
        pctile_raw = res.get("forecast_future").get("percentiles")

        forecast_start_ts = int(datetime.fromisoformat(forecast_start_time).timestamp())

        horizon = self.params["horizon"]
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
        self.forecasted_expires_ts = pctile.get_column("expiration_ts").to_list()
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
        self.fair_payouts = fair_payouts_w_dist.to_pandas().set_index("readable_ts")
        return self.fair_payouts

    def stop_worker(self):
        self.stop_evt.set()
        pass

    def update_spot(self, spot_px):
        fair_payouts = self.calculate_fair_payouts(spot_px=spot_px)

    def start_worker(self):
        self.stop_evt.clear()
        try:
            self.is_running = True
            try:
                self.ctx = mo.current_thread()
            except:
                pass  # worker wasn't started in marimo context
            while not self.stop_evt.is_set():
                print("Fetching From Synth Data")
                price_distributions, current_spot = self.fetch_synth_forecast()
                fair_payouts = self.calculate_fair_payouts(spot_px=current_spot)
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
        self.display_col_seq = "contract,CE-payout,c_fair_payout,c_edge_pct,CE_spot_ts,PE-payout,p_fair_payout,p_edge_pct,PE_spot_ts".split(
            ","
        )
        self.uly_symbol = "frxXAUUSD"
        self._message_counter = 0

    def get_ws_url(self):
        base_url = "https://api.derivws.com"
        otp_url = f"{base_url}/trading/v1/options/accounts/{self.account_id}/otp"
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

        epochs = self.synth_worker.fair_payouts["expiration_ts"].tolist()
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

        mo.output.replace(
            mo.ui.table(
                self.payouts.reset_index()[self.display_col_seq],
                pagination=True,
                page_size=100,
                show_column_summaries=False,
                show_download=False,
                show_data_types=False,
                selection="multi-cell",
                label=f"{self.uly_symbol} Mkt Payout Vs Fair Payout For Rise/Fall Binary Options at current spot",
            )
        )

    def _on_message(self, ws, message):
        try:
            self._message_counter += 1
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
                edge_col = "c_edge_pct" if is_call else "p_edge_pct"

                # Update Local Dataframe
                mkt_payout = msg["proposal"]["payout"]
                self.payouts.loc[expiry_str, p_col] = mkt_payout
                self.payouts.loc[expiry_str, s_col] = spot_time_str
                self.payouts.loc[expiry_str, "uly_symbol"] = req["underlying_symbol"]

                # Lookup fair payout from the SynthWorker data
                if expiry_str in self.synth_worker.fair_payouts.index:
                    fair_payout = round(
                        float(self.synth_worker.fair_payouts.loc[expiry_str, f_col]),
                        2,
                    )
                    self.payouts.loc[expiry_str, f_col] = fair_payout
                    edge = (mkt_payout - fair_payout) / mkt_payout * 100
                    self.payouts.loc[expiry_str, edge_col] = round(edge, 2)

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
