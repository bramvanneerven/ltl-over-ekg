import polars as pl


def load(invoices: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    lines = (
        invoices.select(["PI_Line_No", "PI_No"]).unique().rename({"PI_Line_No": "ID"})
    )
    headers = invoices.select(["PI_No"]).unique().rename({"PI_No": "ID"})

    return lines, headers
