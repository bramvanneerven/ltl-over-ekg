import polars as pl


def load(order_lines: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    return (
        order_lines.select("PO_No").unique().rename({"PO_No": "ID"}),
        order_lines.select(["PO_Line_No", "PO_No"])
        .unique()
        .rename({"PO_Line_No": "ID"}),
    )
