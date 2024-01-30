import polars as pl

import config as cfg
from transform.loader import Loader


def load() -> pl.DataFrame:
    loader = Loader()

    select = [
        "%PI_NO",
        "%PI_LINE_NO",
        "#PI Price",
        "#PI Quantity",
        "PI Posting Date",
        "PI User",
        "PI No",
        "PI Item Type",
    ]

    df = (
        loader.get_df("purchase_invoices", columns=select)
        .with_columns(
            [
                pl.lit("Invoice line created").alias("Activity"),
                pl.col("%PI_NO").fill_null(
                    pl.concat_str(
                        [
                            pl.lit(cfg.company),
                            pl.col("PI No"),
                            pl.col("PI Posting Date").str.split("-").list.get(2),
                        ],
                        separator="-",
                    )
                ),
                (
                    pl.col("PI Posting Date").str.strptime(pl.Datetime)
                    + pl.duration(hours=2)
                ).alias("Timestamp"),
            ]
        )
        .drop("PI No", "PI Posting Date")
        .rename(
            {
                "%PI_NO": "PI_No",
                "%PI_LINE_NO": "PI_Line_No",
                "#PI Price": "Price",
                "#PI Quantity": "Quantity",
                "PI User": "uID",
                "PI Item Type": "Item_Type",
            }
        )
        .unique()
    )

    df_exchange_rates = loader.get_df("manual_exchange_rates")
    df = df.join(df_exchange_rates, on="PI_No", how="left").with_columns(
        [
            pl.col("Exchange_Rate")
            .str.replace_all(",", ".")
            .cast(pl.Float64)
            .alias("Exchange_Rate")
        ]
    )

    return df
