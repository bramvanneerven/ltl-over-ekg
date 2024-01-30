import polars as pl


def load(
    header_changes: pl.DataFrame,
    line_changes: pl.DataFrame,
    line_creation: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    header_change_relation = (
        header_changes.select("PO_No")
        .with_columns(pl.col("PO_No").alias("to_ID"))
        .rename({"PO_No": "from_PO_No"})
        .unique()
    )
    line_change_relation = (
        line_changes.select("PO_Line_No")
        .with_columns(pl.col("PO_Line_No").alias("to_ID"))
        .rename({"PO_Line_No": "from_PO_Line_No"})
        .unique()
    )
    line_creation_relation = (
        line_creation.select("PO_Line_No")
        .unique()
        .with_columns(pl.col("PO_Line_No").alias("to_ID"))
        .rename({"PO_Line_No": "from_PO_Line_No"})
        .unique()
    )
    entity_relation = (
        line_creation.select(["PO_Line_No", "PO_No"])
        .rename({"PO_Line_No": "from_ID", "PO_No": "to_ID"})
        .unique()
    )

    return (
        header_change_relation,
        line_change_relation,
        line_creation_relation,
        entity_relation,
    )
