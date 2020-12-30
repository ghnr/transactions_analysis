import pandas as pd
import numpy as np


class TransactionsAnalysis:
    """
    Analysis class with aggregation methods that operate on a List[Transaction]
    """
    def __init__(self,  transactions_data_path):
        self.df_transactions = self.read_transactions_file(transactions_data_path)
        
    @staticmethod
    def read_transactions_file(data_path):
        """Reads csv file into pandas dataframe"""
        return pd.read_csv(data_path, dtype={"transactionId": str,
                                             "accountId": str,
                                             "transactionDay": np.uint16,
                                             "category": "category",
                                             "transactionAmount": np.float64})
    
    def get_daily_totals(self):
        """Returns sum of transaction amounts grouped by day"""
        return self.df_transactions.groupby("transactionDay") \
                                   .agg({"transactionAmount": "sum"})
    
    def get_average_by_category(self):
        """Returns mean transaction amount grouped by category"""
        return self.df_transactions.groupby(["accountId", "category"]) \
                                   .agg({"transactionAmount": "mean"}) \
                                   .unstack()

    def rolling_window(self, window_size=5, target_total_cols=None):
        """Returns aggregate output of rolling window aggregations"""
        if target_total_cols is None:
            target_total_cols = ["AA", "CC", "FF"]
        
        # +1 to exclude current day in range-start
        window_start = window_size + 1
        # +1 to include the last day in range-end
        window_end = self.df_transactions["transactionDay"].max() + 1
    
        df_all_windows = pd.DataFrame()
    
        for num_day in range(window_start, window_end):
            df_transaction_window = self.df_transactions[(self.df_transactions["transactionDay"] >= num_day - window_size) &
                                                         (self.df_transactions["transactionDay"] < num_day)]
        
            df_aggregated = self.aggregate_max_mean(df_transaction_window)
            df_aggregated = self.aggregate_totals(df_transaction_window, df_aggregated, target_total_cols)
            df_all_windows = df_all_windows.append(self.set_new_col_index(df_aggregated, num_day))
    
        return df_all_windows

    @staticmethod
    def aggregate_max_mean(df: pd.DataFrame):
        """Returns max and mean values of transaction amount by account id"""
        return df.groupby(["accountId"]).agg({"transactionAmount": ["max", "mean"]})

    @staticmethod
    def set_new_col_index(df, day):
        """Returns dataframe with index reset to 'Day'"""
        df["Day"] = day
        return df.reset_index().set_index("Day")

    @staticmethod
    def aggregate_totals(df_window, df_main, target_cols):
        for col in target_cols:
            df_transacs_target = df_window.loc[df_window["category"] == col]
        
            df_transacs_total = df_transacs_target.groupby(["accountId"]) \
                .agg({"transactionAmount": "sum"}) \
                .rename(columns={"transactionAmount": f"{col} Total Value"})

            df_transacs_total.columns = pd.MultiIndex.from_product([["Total Values"], df_transacs_total.columns])
            df_main = df_main.join(df_transacs_total)  # multi-index join
    
        return df_main
    

if __name__ == '__main__':
    
    # Pandas display options used if printing instead of saving
    pd.set_option("display.max_columns", 10)
    pd.set_option("display.width", 1000)
    
    DATA_PATH = "transactions.txt"
    transac_analysis = TransactionsAnalysis(DATA_PATH)

    # Rounding to 2 decimal places
    fmt = '%.2f'
    # Aggregations saved to csv files
    transac_analysis.get_daily_totals().to_csv("daily_totals_.csv", float_format=fmt)
    transac_analysis.get_average_by_category().to_csv("category_averages_.csv", float_format=fmt)
    transac_analysis.rolling_window(5).to_csv("rolling_time_window_.csv", float_format=fmt)
