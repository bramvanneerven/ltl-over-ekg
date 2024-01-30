import polars as pl

from transform.loader import Loader


def load() -> pl.DataFrame:
    loader = Loader()

    select = [
        "PO No",
        "%PO_LINE_NO",
        "#PO Price",
        "#PO Quantity",
        "PO Posting Date",
        "PO User",
        "PO Item Type",
    ]

    df = (
        loader.get_df("purchase_orders")
        .select(select)
        .with_columns(
            [
                pl.col("#PO Price")
                .str.replace_all("€", "")
                .str.replace_all(",", "")
                .str.strip()
                .cast(pl.Float64)
                .alias("#PO Price"),
                pl.lit("Order line created").alias("Activity"),
                pl.col("PO Posting Date").str.strptime(pl.Datetime).alias("Timestamp"),
            ]
        )
        .drop("PO Posting Date")
        .rename(
            {
                "PO No": "PO_No",
                "%PO_LINE_NO": "PO_Line_No",
                "#PO Price": "Price",
                "#PO Quantity": "Quantity",
                "PO User": "uID",
                "PO Item Type": "Item_Type",
            }
        )
        .drop_nulls()
        .unique()
    )

    return df
