# Join data sets

A data set may have more than one dependency. It is then possible to join the data inside the compute function.

## Example 1: using plain Pandas

```python
class EquityAdjustedPrice(DataNode):
    @property
    def calculator_type(self):
        return "pandas"

    @property
    def schema(self):
        return {
            "StockId": (int, ColumnCategory.KEY),
            "AdjustedPrice": (float, ColumnCategory.VALUE),
            "AdjustedPriceUSD": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {
            "equity_raw_price": "EquityRawPrice",
            "fx_rate": DateDependency(
                "FxRate",
                data_fields={"Rate": "FxRate", "BaseCurrency": "BaseCurrency"},
                filters=[[("PriceCurrency", "=", "USD")]],
            ),
        }

    def compute(self, parameters, equity_raw_price, fx_rate):
        equity_raw_price_df = equity_raw_price(ignore_deltas=True)
        fx_rate_df = fx_rate(ignore_deltas=True)
        df = pd.merge(equity_raw_price_df, fx_rate_df, how="inner", left_on=["Currency"], right_on=["BaseCurrency"])
        df["AdjustedPrice"] = df["Price"] * df["AdjustmentFactor"]
        df["AdjustedPriceUSD"] = df["AdjustedPrice"] / df["FxRate"]
        return df
```

## Example 2: using generic functions

```python
class EquityAdjustedPrice(DataNode):
    def set_calculator_type(self, calc_type):
        """Set calculation framework."""
        self.calc_type = calc_type

    @property
    def calculator_type(self):
        return self.calc_type 

    @property
    def schema(self):
        return {
            "StockId": (int, ColumnCategory.KEY),
            "AdjustedPrice": (float, ColumnCategory.VALUE),
            "AdjustedPriceUSD": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {
            "equity_raw_price": "EquityRawPrice",
            "fx_rate": DateDependency(
                "FxRate",
                data_fields={"Rate": "FxRate", "BaseCurrency": "BaseCurrency"},
                filters=[[("PriceCurrency", "=", "USD")]],
            ),
        }

    def compute(self, parameters, equity_raw_price, fx_rate):
        equity_raw_price_df = equity_raw_price(ignore_deltas=True)
        fx_rate_df = fx_rate(ignore_deltas=True)
        df = self.join(equity_raw_price_df, fx_rate_df, on=("Currency", "BaseCurrency"), how="inner")
        df = self.add(df, "AdjustedPrice", self.column(df, "Price") * self.column(df, "AdjustmentFactor"))
        df = self.add(df, "AdjustedPriceUSD", self.column(df, "AdjustedPrice") / self.column(df, "FxRate"))
        return df
```
