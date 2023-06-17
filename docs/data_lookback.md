# Loading data with lookback

When loading data for a specific date, the missing values may be replaced by the last existing values, based on the lookback defined in the node.

By default, the lookback will be applied to each entities in the data set. But it is also possible to have a grouped lookback: in that case, if there is no data at all for a date, then it will be substituted with all the data from the last available date.

## Example 1 (Pandas): price to be defaulted to last available price for each security individually

```python
class EquityPrice(DataNode):
    @property
    def calculator_type(self):
        return "pandas"

    @property
    def schema(self):
        return {
            "StockId": (int, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
        }

    @property
    def lookback(self):
        return 1

    def compute(self, parameters, price):
        ...   # load/compute/return

class EquityPriceLookback1(EquityStock):
    @property
    def calculator_type(self):
        return "pandas"

    @property
    def schema(self):
        return {
            "StockId": (int, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {"EquityPrice": None}  # Will use the default lookback of 1 day, as defined in the EquityPrice class

    # Note: if there is 1 and only 1 dependency and no further transformation to apply to it,
    # then it is not necessary to override the "compute" function.
    # The data from the dependency will be saved as is.


class EquityPriceLookback2(EquityStock):
    @property
    def calculator_type(self):
        return "pandas"

    @property
    def schema(self):
        return {
            "StockId": (int, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {"EquityPrice": DateDependency("EquityPrice", lookback=2)}  # Will override the lookback defined in the EquityPrice class
```

## Example 2 (Pandas): use all T-1 prices if T prices are not available.

Achievable by overriding the "group_lookback" property.

```python
class EquityPrice(DataNode):
    @property
    def calculator_type(self):
        return "pandas"

    @property
    def schema(self):
        return {
            "StockId": (int, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
        }

    @property
    def lookback(self):
        return 1

    @property
    def group_lookback(self):
        return True  # group_lookback is False by default

    def compute(self, parameters, price):
        ...   # load/compute/return

class EquityPriceLookback(EquityStock):
    @property
    def calculator_type(self):
        return "pandas"

    @property
    def schema(self):
        return {
            "StockId": (int, ColumnCategory.KEY),
            "Price": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {"EquityPrice": None}  # Will use the default lookback of 1 day
```
