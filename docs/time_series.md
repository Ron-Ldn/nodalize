# Working with time series

It is possible to specify a dependency to a time series using the WindowDependency class.
The integer parameters represent the offsets from the current date, to get the start and end date.

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
        return {"price": WindowDependency("EquityPrice", -2, 0)}  # will populate all data from 2 days ago until today.

    def compute(self, parameters, price):
        df = price()
        df = df[["StockId", "Price"]].groupby(by=["StockId"]).mean()
        df = df.reset_index()
        df = df.rename(columns={"Price": "PriceAvg"})
        return df        
```
