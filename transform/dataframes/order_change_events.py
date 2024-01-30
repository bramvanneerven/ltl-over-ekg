from pathlib import Path

import polars as pl

import config as cfg
from transform.loader import Loader


def load() -> tuple[pl.DataFrame, pl.DataFrame]:
    loader = Loader()

    df = loader.get_df("prom_p2p_changes")
    activities = Path(cfg.activities_file)

    df_activities = pl.read_excel(activities, sheet_name="Mapping").select(
        ["Activity", "Tabdesc", "Table", "Field"]
    )

    df = df.join(
        df_activities,
        left_on=["CHG TABNAME", "CHG FNAME"],
        right_on=["Table", "Field"],
        how="left",
    ).filter(pl.col("Activity").is_not_null())

    df_grouped = df.with_columns(
        pl.concat_str([pl.col("CHG UDATE"), pl.col("CHG UTIME")], separator=" ")
        .str.strptime(pl.Datetime)
        .alias("Timestamp"),
    ).partition_by("Tabdesc", as_dict=True)

    select = ["CHG OBJECTID", "CHG USERNAME", "Timestamp", "Activity"]
    rename = {
        "CHG OBJECTID": "PO_No",
        "CHG USERNAME": "uID",
    }

    df_lines = (
        df_grouped["Order line"]
        .with_columns(
            pl.concat_str(
                [
                    pl.lit(cfg.company),
                    pl.lit("-"),
                    pl.col("CHG OBJECTID"),
                    pl.lit("-"),
                    pl.lit(0),
                    pl.col("CHG TABKEY").cast(pl.Utf8).str.slice(13),
                ]
            ).alias("PO_Line_No")
        )
        .select(["PO_Line_No", *select])
        .rename(rename)
        .unique()
    )

    df_headers = df_grouped["Order"].select(select).rename(rename).unique()

    return df_headers, df_lines
