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
                data_fields={"Rate": "FxRate", "BaseCurrency": "BaseCurrency"},  # To select sub-set of columns and rename
                filters=[[("PriceCurrency", "=", "USD"), ("DataDate", "=", ParameterValue("DataDate"))]],  # ParameterValue to use a parameter value passed to the compute function
            ),
        }

    def compute(self, parameters, equity_raw_price, fx_rate):
        # Load the data
        equity_raw_price_df = equity_raw_price(ignore_deltas=True)  # ignore_deltas=True to force reload all data and not the last delta updates
        fx_rate_df = fx_rate(ignore_deltas=True)

        # Merge using Pandas
        df = pd.merge(equity_raw_price_df, fx_rate_df, how="inner", left_on=["Currency"], right_on=["BaseCurrency"])

        # Compute
        df["AdjustedPrice"] = df["Price"] * df["AdjustmentFactor"]
        df["AdjustedPriceUSD"] = df["AdjustedPrice"] / df["FxRate"]

        # Return
        return df
```

## Example 2: using generic functions

```python
from nodalize.dependency import ParameterValue

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
                data_fields={"Rate": "FxRate", "BaseCurrency": "Currency"},  # To select sub-set of columns and rename
                filters=[[("PriceCurrency", "=", "USD"), ("DataDate", "=", ParameterValue("DataDate"))]],  # ParameterValue to use a parameter value passed to the compute function
            ),
        }

    def compute(self, parameters, equity_raw_price, fx_rate):
        # The code below is not specific to Pandas. It could be used with either Pandas, Dask, PySpark, Polars or PyArrow

        # Load the data
        equity_raw_price_df = equity_raw_price(ignore_deltas=True)  # ignore_deltas=True to force reload all data and not the last delta updates
        fx_rate_df = fx_rate(ignore_deltas=True)

        # Merge using Pandas
        df = self.calculator.left_join_data_frames(equity_raw_price_df, fx_rate_df, on=["Currency"])

        # Compute
        df = self.calculator.add_column(df, "AdjustedPrice", self.calculator.get_column(df, "Price") * self.calculator.get_column(df, "AdjustmentFactor"))
        df = self.calculator.add_column(df, "AdjustedPriceUSD", self.calculator.get_column(df, "AdjustedPrice") / self.calculator.get_column(df, "FxRate"))

        # Return
        return df
```
