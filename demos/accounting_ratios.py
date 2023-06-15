import os
from datetime import date

import numpy as np
import pandas as pd
import yfinance as yf
from pyarrow import csv

# import sys

# sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from nodalize.constants.column_category import ColumnCategory
from nodalize.data_management.sqlite_data_manager import SqliteDataManager
from nodalize.datanode import DataNode
from nodalize.dependency import DependencyDefinition
from nodalize.orchestration.coordinator import Coordinator


class Tickers(DataNode):
    @property
    def calculator_type(self):
        return "pyarrow"

    @property
    def schema(self):
        return {
            "Ticker": (str, ColumnCategory.KEY),
        }

    def compute(self, parameters):
        tickers_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "tickers.csv"
        )
        return csv.read_csv(tickers_path)


class CompanyData(DataNode):
    @property
    def calculator_type(self):
        return "pandas"

    @property
    def schema(self):
        return {
            "Ticker": (str, ColumnCategory.KEY),
            "Close": (float, ColumnCategory.VALUE),
            "Currency": (str, ColumnCategory.VALUE),
            "Volume": (float, ColumnCategory.VALUE),
            "MarketCap": (float, ColumnCategory.VALUE),
            "BookValue": (float, ColumnCategory.VALUE),
            "NetIncome": (float, ColumnCategory.VALUE),
            "SharesOutstanding": (float, ColumnCategory.VALUE),
            "OperatingCashFlow": (float, ColumnCategory.VALUE),
            "FreeCashFlow": (float, ColumnCategory.VALUE),
            "EnterpriseValue": (float, ColumnCategory.VALUE),
            "EBITDA": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {"tickers": "Tickers"}

    def compute(self, parameters, tickers):
        tickers_df = tickers()

        tickers = []
        closes = []
        volumes = []
        market_caps = []
        currencies = []
        book_values = []
        net_incomes = []
        shares_outstandings = []
        free_cash_flows = []
        operating_cash_flows = []
        enterprise_values = []
        ebitda = []

        for ticker in tickers_df["Ticker"].tolist():
            ticker_proxy = yf.Ticker(ticker)
            data = ticker_proxy.info
            tickers.append(ticker)
            closes.append(data.get("previousClose"))
            volumes.append(data.get("volume"))
            market_caps.append(data.get("marketCap"))
            currencies.append(data.get("currency"))
            book_values.append(data.get("bookValue"))
            net_incomes.append(data.get("netIncomeToCommon"))
            shares_outstandings.append(data.get("sharesOutstanding"))
            free_cash_flows.append(data.get("freeCashflow"))
            operating_cash_flows.append(data.get("operatingCashflow"))
            enterprise_values.append(data.get("enterpriseValue"))
            ebitda.append(data.get("ebitda"))

        return pd.DataFrame(
            {
                "Ticker": tickers,
                "Close": closes,
                "Currency": currencies,
                "Volume": volumes,
                "MarketCap": market_caps,
                "BookValue": book_values,
                "NetIncome": net_incomes,
                "SharesOutstanding": shares_outstandings,
                "OperatingCashFlow": operating_cash_flows,
                "FreeCashFlow": free_cash_flows,
                "EnterpriseValue": enterprise_values,
                "EBITDA": ebitda,
            }
        )


class AccountingRatio(DataNode):
    @property
    def calculator_type(self):
        return "pandas"

    @property
    def numerator(self):
        raise NotImplementedError

    @property
    def denominator(self):
        raise NotImplementedError

    @property
    def schema(self):
        return {
            "Ticker": (str, ColumnCategory.KEY),
            self.__class__.__name__: (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {
            "data": DependencyDefinition(
                "CompanyData",
                data_fields=["Ticker", self.numerator, self.denominator],
            )
        }

    def compute(self, parameters, data):
        data = data()
        data[self.__class__.__name__] = data[self.numerator] / data[self.denominator]
        return data


class EPS(AccountingRatio):
    @property
    def numerator(self):
        return "NetIncome"

    @property
    def denominator(self):
        return "SharesOutstanding"


class PriceToEarnings(DataNode):
    @property
    def calculator_type(self):
        return "pandas"

    @property
    def schema(self):
        return {
            "Ticker": (str, ColumnCategory.KEY),
            "PriceToEarnings": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {
            "data": DependencyDefinition(
                "CompanyData", data_fields=["Ticker", "Close"]
            ),
            "eps": DependencyDefinition(
                "EPS", data_fields=["Ticker", "EPS"]
            ),
        }

    def compute(self, parameters, data, eps):
        data = data()
        eps = eps()
        merged_data = pd.merge(data, eps, how="inner", on="Ticker")
        merged_data["PriceToEarnings"] = merged_data["Close"] / merged_data["EPS"]
        return merged_data


class PriceToCashFlow(AccountingRatio):
    @property
    def numerator(self):
        return "Close"

    @property
    def denominator(self):
        return "OperatingCashFlow"


class PriceToFreeCashFlow(AccountingRatio):
    @property
    def numerator(self):
        return "Close"

    @property
    def denominator(self):
        return "FreeCashFlow"


class EvToEbitda(AccountingRatio):
    @property
    def numerator(self):
        return "EnterpriseValue"

    @property
    def denominator(self):
        return "EBITDA"


class PriceToBook(AccountingRatio):
    @property
    def numerator(self):
        return "EnterpriseValue"

    @property
    def denominator(self):
        return "BookValue"


class Rank(DataNode):
    @property
    def calculator_type(self):
        return "pandas"

    def __init__(self, calculator_factory, data_manager_factory, ratio_name, **kwargs):
        self._ratio_name = ratio_name
        super().__init__(calculator_factory, data_manager_factory, **kwargs)

    @property
    def identifier(self):
        return self._ratio_name + "Rank"

    @property
    def schema(self):
        return {
            "Ticker": (str, ColumnCategory.KEY),
            self._ratio_name + "Rank": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {"ratio": self._ratio_name}

    def compute(self, parameters, ratio):
        df = ratio(ignore_deltas=True)
        df[self._ratio_name] = np.where(
            df[self._ratio_name] < 0, np.NAN, df[self._ratio_name]
        )
        df[self._ratio_name + "Rank"] = np.trunc(df[self._ratio_name].rank())
        return df


coordinator = Coordinator("test")
coordinator.set_data_manager(
    "sqlite", SqliteDataManager("TestDb/test.db"), default=True
)
coordinator.create_data_node(Tickers)
coordinator.create_data_node(CompanyData)
coordinator.create_data_node(EPS)
coordinator.create_data_node(PriceToEarnings)
coordinator.create_data_node(PriceToCashFlow)
coordinator.create_data_node(PriceToFreeCashFlow)
coordinator.create_data_node(EvToEbitda)
coordinator.create_data_node(PriceToBook)
coordinator.create_data_node(Rank, ratio_name="PriceToEarnings")
coordinator.create_data_node(Rank, ratio_name="PriceToCashFlow")
coordinator.create_data_node(Rank, ratio_name="PriceToFreeCashFlow")
coordinator.create_data_node(Rank, ratio_name="EvToEbitda")
coordinator.create_data_node(Rank, ratio_name="PriceToBook")
coordinator.set_up()

coordinator.run_recursively(
    node_identifiers=["Tickers"],
    global_parameters={"DataDate": date(2023, 4, 14)},
)

priceToBook = coordinator.get_data_node("PriceToBook")
priceToBookDf = priceToBook.load(columns=["Ticker", "PriceToBook"])

priceToBookRank = coordinator.get_data_node("PriceToBookRank")
rankDf = priceToBookRank.load(
    columns=["Ticker", "PriceToBookRank"],
    filters=[[("DataDate", "=", date(2023, 4, 14)), ("PriceToBookRank", "<", 10)]],
)

data = pd.merge(rankDf, priceToBookDf, how="inner", on="Ticker").sort_values(
    by="PriceToBookRank"
)

print(data)
