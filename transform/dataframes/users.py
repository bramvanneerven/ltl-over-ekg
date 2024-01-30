import polars as pl


def load_entities(
    events: list[pl.DataFrame],
) -> pl.DataFrame:
    users_dfs = [df.select("uID").unique() for df in events]

    users = pl.concat(users_dfs).unique().rename({"uID": "ID"})

    return users


def load_relations(events: pl.DataFrame) -> pl.DataFrame:
    return (
        events.select("uID")
        .unique()
        .with_columns(pl.col("uID").alias("to_uID"))
        .rename({"uID": "from_ID"})
    )
