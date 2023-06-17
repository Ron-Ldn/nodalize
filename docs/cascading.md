# Cascading calculations

When using *Coordinator.run_recursively*, delta updates will be propagated to downstream nodes. In that way, the downstream calculations won't need to reload everything, and the only the deltas will propagate.

However, some calculations cannot work from delta updates only. As an examples, a ranking calculation would need all upstream data, not only those impacted by some event.
It is possible to force loading all data using the parameters "ignore_deltas".

## Example:

```python
class EquityPriceMove(DataNode):
    @property
    def schema(self):
        return {
            "StockId": (int, ColumnCategory.KEY),
            "PriceMove": (float, ColumnCategory.VALUE),
        }

    @property
    def calculator_type(self) -> str:
        "pandas"

    @property
    def dependencies(self):
        return {
            "lagged_price": LagDependency("EquityPrice", 1, data_fields={
                    "StockId": "StockId",
                    "Price": "PrevPrice",  # Columns can be renamed in the parent data frame
                    "DataDate": "DataDate",
                },
            ),
            "equity_price": "EquityPrice",
        }

    def compute(self, parameters, lagged_price, equity_price):
        lagged_price_df = lagged_price(ignore_deltas=True)  # Ignore delta updates and reload all
        equity_price_df = equity_price()  # By default, will load only the entities run_recursively by the parent.

        df = pd.merge(lagged_price_df, equity_price_df, how="inner", on=["StockId", "DataDate"]) # Inner join so only the securities in equity_price_df will remain.

        return self.calculator.add_column(
            df,
            "PriceMove",
            (self.calculator.get_column(df, "Price") - self.calculator.get_column(df, "PrevPrice"))
            / self.calculator.get_column(df, "PrevPrice"),
        )

coordinator.create_data_node(EquityPrice)
coordinator.create_data_node(EquityPriceMove).set_calculator_type(calc_type)
coordinator.set_up()

# If the "EquityPrice" node refreshes only 2 securities, then only these 2 securities will be
# loaded by "equity_price()".
coordinator.run_recursively(
    node_identifiers=["EquityPrice"],
    global_parameters={"DataDate": date(2022, 1, 1)},
)
```
