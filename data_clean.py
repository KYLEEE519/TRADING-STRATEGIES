import pandas as pd

class KlineProcessor:
    def __init__(self, file_path, encoding="GBK"):
        """
        初始化类，读取 CSV 文件并转换为 DataFrame。
        
        :param file_path: CSV 文件路径
        :param encoding: 文件编码，默认为 "GBK"
        """
        self.file_path = file_path
        self.encoding = encoding
        self.df = self._load_data()

    def _load_data(self):
        """ 读取 CSV 文件并转换时间戳列 """
        df = pd.read_csv(self.file_path, encoding=self.encoding)
        df["timestamp"] = pd.to_datetime(df["created_time/成交时间"], unit="ms")
        df["minute"] = df["timestamp"].dt.floor("T")  # 按分钟对齐
        return df

    def get_kline_df(self):
        """ 计算分钟级 K 线并返回 DataFrame """
        kline = self.df.groupby("minute").agg(
            open=("price/价格", "first"),  # 开盘价
            high=("price/价格", "max"),    # 最高价
            low=("price/价格", "min"),     # 最低价
            close=("price/价格", "last"),  # 收盘价
            vol=("size/数量", "sum")       # 成交量
        ).reset_index()

        # 重新命名时间列
        kline.rename(columns={"minute": "timestamp"}, inplace=True)
        return kline
