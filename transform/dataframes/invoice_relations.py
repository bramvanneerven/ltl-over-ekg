import polars as pl

from transform.loader import Loader


def load(
    invoices: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    loader = Loader()

    creation_of = (
        invoices.select("PI_Line_No")
        .unique()
        .with_columns(pl.col("PI_Line_No").alias("to_ID"))
        .rename({"PI_Line_No": "from_PI_Line_No"})
    )

    invoice_for = (
        loader.get_df("linktable_purchases", columns=["%PI_LINE_NO", "%PO_LINE_NO"])
        .drop_nulls()
        .unique()
        .rename({"%PI_LINE_NO": "from_ID", "%PO_LINE_NO": "to_ID"})
    )

    part_of = (
        invoices.select(["PI_No", "PI_Line_No"])
        .drop_nulls()
        .unique()
        .rename({"PI_No": "to_ID", "PI_Line_No": "from_ID"})
    )

    booked_as = (
        loader.get_df("linktable_purchases", columns=["%PI_LINE_NO", "%GL_DOC"])
        .drop_nulls()
        .unique()
        .rename({"%PI_LINE_NO": "from_ID"})
        .join(part_of, on="from_ID", how="left")
        .drop("from_ID")
        .rename({"to_ID": "from_ID", "%GL_DOC": "to_ID"})
        .unique()
    )

    return creation_of, booked_as, invoice_for, part_of
