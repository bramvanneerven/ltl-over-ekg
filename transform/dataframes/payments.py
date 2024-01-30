import polars as pl

import config as cfg
from transform.loader import Loader


def load() -> (
    tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]
):
    loader = Loader()

    payments = (
        loader.get_df("payments", dtypes={"%INVOICE": pl.Utf8})
        .drop_nulls(subset=["%BKPF", "%INVOICE"])
        .unique()
        .filter(pl.col("Invoice type").str.contains("Factuur bruto"))
        .with_columns(
            [
                pl.concat_str(
                    [
                        pl.lit(cfg.company),
                        pl.col("%BKPF"),
                        pl.col("%INVOICE").cast(pl.Utf8).str.slice(-4),
                    ],
                    separator="-",
                ).alias("GL_Doc"),
                pl.col("Timestamp")
                .str.split_exact(" ", 1)
                .struct.rename_fields(["Date", "Time"]),
            ]
        )
        .unnest("Timestamp")
        .with_columns(
            [
                pl.concat_str([pl.col("Date"), pl.col("Time")], separator=" ")
                .str.strptime(pl.Datetime)
                .alias("Timestamp"),
                pl.when(pl.col("%INVOICE").str.contains(cfg.company))
                .then(
                    pl.concat_str(
                        [
                            pl.lit(cfg.company),
                            pl.col("%INVOICE").str.slice(0, 10),
                            pl.col("%INVOICE").str.slice(-4),
                        ],
                        separator="-",
                    )
                )
                .otherwise(
                    pl.concat_str(
                        [
                            pl.lit(cfg.company),
                            pl.col("%INVOICE").str.slice(0, 10),
                            pl.col("Date").str.split("-").list.get(2),
                        ],
                        separator="-",
                    )
                )
                .alias("PI_No"),
            ]
        )
        .select(
            ["GL_Doc", "PI_No", "Activity", "Timestamp", "Resource", "Invoice type"]
        )
        .rename({"Resource": "uID", "Invoice type": "Invoice_Type"})
        .unique()
    )

    events = payments.select(
        ["GL_Doc", "uID", "Invoice_Type", "Activity", "Timestamp"]
    ).unique("GL_Doc")

    entities = payments.select("GL_Doc").unique().rename({"GL_Doc": "ID"})

    relation_creation_of = (
        payments.select("GL_Doc")
        .unique()
        .with_columns(pl.col("GL_Doc").alias("to_ID"))
        .rename({"GL_Doc": "from_GL_Doc"})
    )

    relation_booked_as = (
        payments.select("GL_Doc")
        .unique()
        .with_columns(pl.col("GL_Doc").alias("to_ID"))
        .rename({"GL_Doc": "from_ID"})
    )

    relation_payment_for = (
        payments.select(["GL_Doc", "PI_No"])
        .unique()
        .rename({"GL_Doc": "from_ID", "PI_No": "to_ID"})
    )

    return (
        events,
        entities,
        relation_creation_of,
        relation_booked_as,
        relation_payment_for,
    )
