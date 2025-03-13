import pandas as pd
import numpy as np

class ChanAndSmaCombinedStrategy:
    """
    将简单均线策略与缠中说禅“趋势平均力度”思路相结合：
    1) 依然用短期-长期均线（5/20）的交叉来判别多头/空头大趋势；
    2) 同时计算中短均线（5/10）的“趋势平均力度”：
       - 一旦发现本段趋势比上一段同方向趋势更弱，可视为“背驰”信号，从而平仓。
    """

    def __init__(self, short_window=5, medium_window=10, long_window=20):
        """
        :param short_window: 短期均线窗口(默认5)
        :param medium_window: 中短均线窗口(默认10)，用于趋势力度计算
        :param long_window: 长期均线窗口(默认20)
        """
        self.short_window = short_window
        self.medium_window = medium_window
        self.long_window = long_window

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算并返回包含以下列的 DataFrame:
        - short_ma: 短期均线
        - medium_ma: 中短均线
        - long_ma: 长期均线
        - diff_5_10: 短中均线差 (MA5 - MA10)
        - trend_avg_strength: 用于模拟“趋势平均力度”的一个示例值
        - signal: 交易信号 (1 表示持多, 0 表示空仓)
        """
        # 1) 计算基础均线
        df["short_ma"] = df["close"].rolling(self.short_window).mean()
        df["medium_ma"] = df["close"].rolling(self.medium_window).mean()
        df["long_ma"] = df["close"].rolling(self.long_window).mean()

        # 2) 简单均线交叉信号（5 上穿 20 -> 做多, 下穿 -> 平仓）
        df["signal"] = 0
        df.loc[df["short_ma"] > df["long_ma"], "signal"] = 1
        df.loc[df["short_ma"] <= df["long_ma"], "signal"] = 0

        # 3) 引入“缠中说禅”思路中的短-中均线之差 (MA5 - MA10)
        df["diff_5_10"] = df["short_ma"] - df["medium_ma"]

        # -- 以下是一个简化的“趋势平均力度”示例做法 --
        # 3.1 先定义当前短-中均线差的符号(多头 or 空头)
        df["diff_sign"] = np.where(df["diff_5_10"] > 0, 1, -1)

        # 3.2 识别从上一次翻转(sign变化)到现在的区间，用来模拟累加
        #     也就是“从上一吻到这一吻”的概念
        df["sign_change"] = df["diff_sign"] * df["diff_sign"].shift(1)
        # sign_change < 0 表示发生了由正到负或负到正的转换
        # 我们可以用 cumsum() 的方式，给同一“波段”打上相同的 ID
        df["wave_id"] = (df["sign_change"] < 0).cumsum()

        # 3.3 对每个 wave_id 计算“diff_5_10 的累加均值”，模拟“波段平均力度”
        #     在实际中，你也可改成更加贴近缠论公式的滚动累加法
        wave_strength = (
            df.groupby("wave_id")["diff_5_10"]
            .apply(lambda x: x.sum() / max(len(x), 1))
            .rename("wave_strength")
        )
        # 合并回原 DataFrame
        df = df.merge(wave_strength, left_on="wave_id", right_index=True)

        # 3.4 为方便与前一波的强度比较，用 shift(1) 看“上一段波段强度”
        df["prev_wave_strength"] = df["wave_strength"].shift(1)

        # 4) 利用“背驰(波段强度减弱)”来辅助平仓或提前退出
        #    示例：若当前还是多头(signal=1)，但本波段强度比上一波弱，则及时平仓
        #    你也可以加更多条件过滤，以减少假信号
        condition_weaker = (
            (df["signal"] == 1) &
            (df["diff_sign"] == 1) &  # 短中均线仍是多头区间
            (df["wave_strength"] < df["prev_wave_strength"])  # 当前平均力度 < 前一波
        )
        # 对出现背驰迹象的情况，把 signal 调整为 0（平多）
        df.loc[condition_weaker, "signal"] = 0

        # 返回包含新指标与信号的 DataFrame
        return df


# ------------------ 以下为调用演示 ------------------
if __name__ == "__main__":
    # 举例：假设 df 里至少包含 "close" 列
    data = {
        "close": [100, 102, 103, 101, 99, 98, 100, 101, 105, 107, 110, 109,
                  108, 106, 107, 111, 115, 120, 119, 118, 116, 117, 115]
    }
    df_sample = pd.DataFrame(data)

    strategy = ChanAndSmaCombinedStrategy(short_window=5, medium_window=10, long_window=20)
    result_df = strategy.generate_signals(df_sample)

    print(result_df[[
        "close", "short_ma", "medium_ma", "long_ma",
        "diff_5_10", "wave_id", "wave_strength",
        "prev_wave_strength", "signal"
    ]])