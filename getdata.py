# getdata.py
import pandas as pd
import time
import threading
from datetime import datetime
from okx import MarketData

class OKXDataFetcher:
    def __init__(self, instId="BTC-USDT"):
        self.instId = instId
        self.df = None
        self._initialize_api()

    def _initialize_api(self):
        """即使公共API也需要基础配置"""
        self.market = MarketData.MarketAPI(
            api_key="",
            api_secret_key="",
            passphrase="",
            flag="0"  # 0: 实盘 1: 模拟盘
        )

    def fetch_1m_data(self, days=1):
        """
        一次性获取过去 N 天的 1m K线数据 (最多 1440 * days 条)
        """
        total_limit = 1440 * days
        all_data = []
        after = None
        retry = 0
        max_retries = 3

        print(f"▶️ 开始获取 {self.instId} {days} 天数据...")

        while len(all_data) < total_limit and retry < max_retries:
            try:
                params = {
                    "instId": self.instId,
                    "bar": "1m",
                    "limit": min(300, total_limit - len(all_data))
                }
                if after is not None:
                    params["after"] = str(after)

                resp = self.market.get_candlesticks(**params)
                if resp.get("code") != "0":
                    print(f"⚠️ API错误: {resp.get('msg')}")
                    retry += 1
                    time.sleep(1)
                    continue

                batch = resp.get("data", [])
                if not batch:
                    print("✅ 已获取全部可用数据")
                    break

                all_data.extend(batch)
                oldest = batch[-1]
                oldest_ts = int(oldest[0])
                after = oldest_ts - 1

                print(
                    f"  已获取 {len(batch):3d} 条，累计 {len(all_data):4d}/{total_limit}",
                    end="\r"
                )
                time.sleep(0.15)
                retry = 0

            except Exception as e:
                print(f"🔴 请求异常: {str(e)}")
                retry += 1
                time.sleep(2 ** retry)

        # 整理成 DataFrame
        if all_data:
            columns = [
                "timestamp", "open", "high", "low", "close",
                "vol", "volCcy", "volCcyQuote", "confirm"
            ]

            full_df = pd.DataFrame(all_data, columns=columns)
            self.df = full_df[["timestamp", "open", "high", "low", "close", "vol"]].copy()

            numeric_cols = ["open", "high", "low", "close", "vol"]
            self.df[numeric_cols] = self.df[numeric_cols].apply(pd.to_numeric, errors="coerce")

            self.df["timestamp"] = pd.to_datetime(
                pd.to_numeric(self.df["timestamp"]),
                unit="ms",
                utc=True
            ).dt.tz_convert(None)

            self.df = self.df.drop_duplicates(subset=["timestamp"])
            self.df = self.df.sort_values("timestamp").reset_index(drop=True)

            print(f"\n✅ 最终获取 {len(self.df)} 条有效数据")
            if not self.df.empty:
                start_time = self.df.timestamp.iloc[0]
                end_time = self.df.timestamp.iloc[-1]
                print(f"⏰ 时间范围: {start_time} 至 {end_time}")
        else:
            print("❌ 未获取到任何有效数据")

    def fetch_latest_data(self):
        """
        获取最新 1m K线数据并更新 self.df（滚动窗口）
        """
        try:
            params = {
                "instId": self.instId,
                "bar": "1m",
                "limit": 1
            }
            resp = self.market.get_candlesticks(**params)
            if resp.get("code") != "0":
                print(f"⚠️ API错误: {resp.get('msg')}")
                return

            latest = resp.get("data", [])[0]
            if not latest:
                print("❌ 未获取到最新数据")
                return

            latest_data = pd.DataFrame(
                [latest],
                columns=["timestamp", "open", "high", "low", "close", "vol", "volCcy", "volCcyQuote", "confirm"]
            )
            latest_data = latest_data[["timestamp", "open", "high", "low", "close", "vol"]]
            numeric_cols = ["open", "high", "low", "close", "vol"]
            latest_data[numeric_cols] = latest_data[numeric_cols].apply(pd.to_numeric, errors="coerce")
            latest_data["timestamp"] = pd.to_datetime(
                pd.to_numeric(latest_data["timestamp"]), unit="ms", utc=True
            ).dt.tz_convert(None)

            if self.df is not None and not self.df.empty:
                last_timestamp = self.df["timestamp"].iloc[-1]
                new_timestamp = latest_data["timestamp"].iloc[0]
                if last_timestamp == new_timestamp:
                    # 替换最后一行，避免重复
                    self.df.iloc[-1] = latest_data.iloc[0]
                    return

            # 正常追加
            if self.df is None or self.df.empty:
                self.df = latest_data
            else:
                self.df = pd.concat([self.df, latest_data]).drop_duplicates("timestamp").sort_values("timestamp")
                if len(self.df) > 1440:
                    self.df = self.df.iloc[-1440:]

            new_close = latest_data["close"].iloc[0]
            new_time = latest_data["timestamp"].iloc[0]
            print(f"✅ 新增数据: {new_time} | 收盘价={new_close}")

        except Exception as e:
            print(f"🔴 请求异常: {str(e)}")

    def start_real_time_fetch(self):
        """
        每分钟倒数第三秒（58s）获取最新数据
        """
        def fetch_loop():
            while True:
                now = datetime.utcnow()
                if now.second == 58:
                    self.fetch_latest_data()
                    time.sleep(1)
                time.sleep(0.5)

        threading.Thread(target=fetch_loop, daemon=True).start()

    def get_cleaned_data(self):
        """返回最新的 DataFrame"""
        return self.df.copy() if self.df is not None else None
