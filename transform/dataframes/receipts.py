import polars as pl

from transform.loader import Loader


def load() -> (
    tuple[
        pl.DataFrame,
        pl.DataFrame,
        pl.DataFrame,
        pl.DataFrame,
        pl.DataFrame,
        pl.DataFrame,
        pl.DataFrame,
    ]
):
    loader = Loader()

    events = (
        loader.get_df(
            "purchase_receipts",
            columns=[
                "%PR_LINE_NO",
                "#PR Quantity",
                "PR Item Type",
                "PR Receipt Type",
                "PR Posting Date",
                "PR User",
            ],
        )
        .with_columns(
            [
                (
                    pl.concat_str(
                        [
                            pl.lit("Purchase receipt created: "),
                            pl.col("PR Receipt Type"),
                        ]
                    ).alias("Activity")
                ),
                (
                    pl.col("PR Posting Date").str.strptime(pl.Datetime)
                    + pl.duration(hours=1)
                ).alias("Timestamp"),
            ]
        )
        .drop("PR Posting Date")
        .rename(
            {
                "%PR_LINE_NO": "PR_Line_No",
                "#PR Quantity": "Quantity",
                "PR Item Type": "Item_Type",
                "PR Receipt Type": "PR_Receipt_Type",
                "PR User": "uID",
            }
        )
    )

    line_entities = events.select("PR_Line_No").unique().rename({"PR_Line_No": "ID"})

    relation_part_of = (
        loader.get_df(
            "purchase_receipts",
            columns=["%PR_LINE_NO", "PR Company Code", "PR No", "PR Fiscal Year"],
        )
        .with_columns(
            [
                pl.concat_str(
                    [
                        pl.col("PR Company Code"),
                        pl.col("PR No"),
                        pl.col("PR Fiscal Year"),
                    ],
                    separator="-",
                ).alias("ID")
            ]
        )
        .select(["%PR_LINE_NO", "ID"])
        .rename({"%PR_LINE_NO": "from_ID", "ID": "to_ID"})
        .unique()
    )

    header_entities = relation_part_of.select("to_ID").unique().rename({"to_ID": "ID"})

    relation_for = (
        loader.get_df(
            "linktable_purchases", columns=["%PR_LINE_NO", "%PO_LINE_NO", "%PI_LINE_NO"]
        )
        .filter(pl.col("%PI_LINE_NO").is_not_null())
        .drop("%PI_LINE_NO")
        .rename({"%PR_LINE_NO": "from_ID", "%PO_LINE_NO": "to_ID"})
        .unique()
        .drop_nulls()
    )

    relation_booked_as = (
        loader.get_df(
            "journals",
            columns=["%GL_DOC", "%GL_PI"],
        )
        .filter(pl.col("%GL_PI").is_in(header_entities["ID"]))
        .rename({"%GL_PI": "from_ID", "%GL_DOC": "to_ID"})
        .unique()
    )

    relation_creation = (
        events.select("PR_Line_No")
        .unique()
        .with_columns(pl.col("PR_Line_No").alias("to_ID"))
        .rename({"PR_Line_No": "from_PR_Line_No"})
    )

    return (
        events,
        header_entities,
        line_entities,
        relation_creation,
        relation_booked_as,
        relation_for,
        relation_part_of,
    )
