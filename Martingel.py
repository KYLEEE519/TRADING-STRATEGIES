import pandas as pd

class MartingaleStrategy:
    def __init__(self, df, leverage_list, position_list):
        self.df = df.copy()
        self.leverage_list = leverage_list
        self.position_list = position_list

        self.df['open_signal'] = 0
        self.df['close_signal'] = 0

        self.position = 0
        self.entry_price = 0
        self.total_loss = 0
        self.total_profit = 0
        self.current_layer = 0
        self.direction = None

        self.x = self.calculate_4h_volatility()
        self.y = self.calculate_1h_volatility()

    def calculate_4h_volatility(self):
        last_24h = self.df[-24*60:]  # 最近24小时
        resampled = last_24h.resample('4h', on='timestamp').agg({'high': 'max', 'low': 'min'})
        return max((resampled['high'] - resampled['low']) / resampled['low'])

    def calculate_1h_volatility(self):
        last_24h = self.df[-24*60:]
        resampled = last_24h.resample('1h', on='timestamp').agg({'high': 'max', 'low': 'min'})
        return max((resampled['high'] - resampled['low']) / resampled['low'])

    def check_entry_condition(self, row):
        if self.x <= 0.01:
            self.df.at[i, 'condition_log'] += "条件1未满足: 波动率x<=1%; "
        return False
        
        if self.x <= 0.01:
            self.df.at[i, 'condition_log'] += "条件1未满足: 波动率x<=1%; "
            return False

        if row['MA13'] > row['MA120']:
            self.direction = 'long'
            self.df.at[i, 'condition_log'] += "条件2满足: MA13>MA120 做多; "
            return True
        elif row['MA13'] < row['MA120']:
            self.direction = 'short'
            self.df.at[i, 'condition_log'] += "条件2满足: MA13<MA120 做空; "
            return True
        else:
            self.df.at[i, 'condition_log'] += "条件2未满足: MA13和MA120无明显趋势; "
            return False


    def calculate_stop_loss(self, layer):
        volatility_factor = min((1 / 2) * self.x, self.y)
        return volatility_factor * self.leverage_list[layer]

    def calculate_take_profit(self, layer):
        if layer == 0:
            return self.leverage_list[layer] * 0.01  # 第一单固定止盈
        return min((1 / 2) * self.x, self.y) * self.leverage_list[layer]

    def add_position(self, price, layer, index):
        new_position = self.position_list[layer]
        self.entry_price = (self.entry_price * self.position + price * new_position) / (self.position + new_position)
        self.position += new_position
        self.current_layer = layer
        self.df.loc[index, 'open_signal'] = 1 if self.direction == 'long' else -1
        print(f"【加仓】第{layer+1}层，价格{price:.2f}，均价{self.entry_price:.2f}，总仓位{self.position:.2%}")

    def check_slope_reversal(self, ma13_series):
        if len(ma13_series) < 6:
            return False
        return (ma13_series.iloc[-1] - ma13_series.iloc[-6]) * (ma13_series.iloc[-2] - ma13_series.iloc[-7]) < 0

    def check_take_profit(self, price):
        profit = (price - self.entry_price) / self.entry_price if self.direction == 'long' else (self.entry_price - price) / self.entry_price
        target_profit = self.calculate_take_profit(self.current_layer)
        return profit >= target_profit

    def check_stop_loss(self, price):
        loss = (self.entry_price - price) / self.entry_price if self.direction == 'long' else (price - self.entry_price) / self.entry_price
        target_loss = self.calculate_stop_loss(self.current_layer)
        return loss >= target_loss or self.total_loss >= 0.5

    def run(self):
        for i, row in self.df.iterrows():
            price = row['close']
            recent_ma13 = self.df['MA13'].iloc[max(0, i-5):i+1]

            if self.position == 0:
                if self.check_entry_condition(row):
                    self.add_position(price, 0, i)
                    print(f"【开单】方向：{self.direction}，价格：{price:.2f}")
            else:
                if self.check_take_profit(price) and self.check_slope_reversal(recent_ma13):
                    print(f"【止盈】价格：{price:.2f}，层数：{self.current_layer+1}")
                    self.df.loc[i, 'close_signal'] = 1
                    self.total_profit += price - self.entry_price
                    self.position = 0
                    self.current_layer = 0
                    continue

                if self.check_stop_loss(price) and self.check_slope_reversal(recent_ma13):
                    if self.current_layer < len(self.leverage_list) - 1:
                        self.add_position(price, self.current_layer + 1, i)
                    else:
                        print(f"【爆仓】价格：{price:.2f}，总亏损达到50%")
                        self.df.loc[i, 'close_signal'] = 1
                        self.position = 0
                        self.current_layer = 0
                        break

    def get_strategy_df(self):
        return self.df
