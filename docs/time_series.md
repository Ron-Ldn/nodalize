# Working with time series

It is possible to specify a dependency to a time series using the *WindowDependency* class. The integer parameters represent the offsets from the current date, to get the start and end date.

When computing recursively (*Coordinator.run_recursively*), *WindowDependency* will ignore temporal dependencies. Let's take the example below, where *EquityPriceAverage* calculated on date T will depend on a year of data from *EquityPrice*. An update of *EquityPrice* for date T will only update *EquityPriceAverage* for date T (instead of updating a year forward).

## Example: using plain Pandas

```python
from nodalize.custom_dependencies.window import WindowDependency

class EquityPriceAverage(DataNode):
    @property
    def calculator_type(self):
        return "pandas"

    @property
    def schema(self):
        return {
            "StockId": (int, ColumnCategory.KEY),
            "PriceAvg": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {"price": WindowDependency("EquityPrice", -365, 0)}  # will populate all data from 365 days ago until today.

    def compute(self, parameters, price):
        df = price()
        df = df[["StockId", "Price"]].groupby(by=["StockId"]).mean()
        df = df.reset_index()
        df = df.rename(columns={"Price": "PriceAvg"})
        return df        
```
