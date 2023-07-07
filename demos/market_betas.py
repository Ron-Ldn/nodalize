import os
from abc import ABC
from datetime import date, timedelta

import numpy as np
import pandas as pd
import yfinance as yf

import sys

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from nodalize.constants.column_category import ColumnCategory
from nodalize.custom_dependencies.lag import WeekDayLagDependency
from nodalize.custom_dependencies.window import WindowDependency
from nodalize.data_management.kdb_data_manager import KdbDataManager
from nodalize.datanode import DataNode
from nodalize.orchestration.coordinator import Coordinator


class PandasNode(ABC, DataNode):
    @property
    def value_columns(self):
        return {}

    @property
    def schema(self):
        keys = {"Ticker": (str, ColumnCategory.KEY)}
        final_schema = {**keys, **self.value_columns}
        return final_schema


class Tickers(PandasNode):
    def compute(self, parameters):
        tickers_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "tickers.csv"
        )
        return pd.read_csv(tickers_path)


class AdjustedPriceClose(PandasNode):
    @property
    def dependencies(self):
        return {"tickers": "Tickers"}

    @property
    def value_columns(self):
        return {"Price": (float, ColumnCategory.VALUE)}

    def compute(self, parameters, tickers):
        tickers_df = tickers()
        tickers = tickers_df["Ticker"].tolist()
        tickers.append("^GSPC")  # S&P500

        end_dt = parameters["DataDate"]

        if parameters.get("mode") == "backfill":
            start_dt = end_dt - timedelta(365)
        else:
            start_dt = end_dt

        raw_data = yf.download(
            tickers=" ".join(tickers),
            start=start_dt.strftime("%Y-%m-%d"),
            end=end_dt.strftime("%Y-%m-%d"),
            interval="1d",
        )

        columns = [("Adj Close", t) for t in tickers]
        raw_data = raw_data[columns]
        raw_data.columns = raw_data.columns.droplevel()
        final_data = raw_data.reset_index().melt(id_vars=["Date"], value_vars=tickers)
        final_data.columns = ["DataDate", "Ticker", "Price"]
        return final_data


class Return(PandasNode):
    @property
    def dependencies(self):
        return {
            "priceT": "AdjustedPriceClose",
            "priceTMinus1": WeekDayLagDependency(
                "AdjustedPriceClose",
                day_lag=1,
                data_fields={"Ticker": "Ticker", "Price": "LaggedPrice"},
            ),
        }

    @property
    def value_columns(self):
        return {
            "Return": (float, ColumnCategory.VALUE),
        }

    def compute(self, parameters, priceT, priceTMinus1):
        df = priceT()
        lagged = priceTMinus1()
        df = pd.merge(df, lagged, on="Ticker", how="inner")
        df["Return"] = (df["Price"] - df["LaggedPrice"]) / df["LaggedPrice"]
        return df


class Beta(PandasNode):
    @property
    def dependencies(self):
        return {"returns": WindowDependency("Return", start_day_offset=-365)}

    @property
    def value_columns(self):
        return {"Beta": (float, ColumnCategory.VALUE)}

    def compute(self, parameters, returns):
        all_returns = returns()

        s_and_p_returns = all_returns.loc[all_returns["Ticker"] == "^GSPC", "Return"]
        s_and_p_var = np.var(s_and_p_returns.to_numpy())

        tickers = [t for t in all_returns["Ticker"].unique() if t != "^GSPC"]
        all_returns = all_returns.pivot(
            index="DataDate", columns="Ticker", values="Return"
        )

        betas = {}
        for t in tickers:
            cov = np.cov(all_returns["^GSPC"], all_returns[t])[0][1]
            beta = cov / s_and_p_var
            betas[t] = beta

        ret = pd.DataFrame({"Ticker": list(betas.keys()), "Beta": list(betas.values())})

        return ret


coordinator = Coordinator("test")
coordinator.set_data_manager(
    "kdb", KdbDataManager(None, "localhost", 5000, lambda: (None, None)), default=True
)
coordinator.set_calculator("pandas", default=True)
coordinator.create_data_node(Tickers)
coordinator.create_data_node(AdjustedPriceClose)
coordinator.create_data_node(Return)
coordinator.create_data_node(Beta)
coordinator.set_up()

coordinator.run_recursively(
    node_identifiers=["Tickers"],
    global_parameters={"DataDate": date(2023, 5, 25)},
    specific_parameters={"AdjustedPriceClose": {"mode": "backfill"}},
)


print(coordinator.get_data_node("Beta").load())
