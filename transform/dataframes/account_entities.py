import polars as pl

from transform.loader import Loader


def load(
    journal_entry_modified_relations: list[pl.DataFrame],
) -> pl.DataFrame:
    loader = Loader()

    dfs = [df.select("to_ID").unique() for df in journal_entry_modified_relations]

    accounts = pl.concat(dfs).unique()

    account_descriptions = loader.get_df(
        "gl_master_data", columns=["%GL_ACC", "GL Acc Desc", "GL Acc Type"]
    )

    accounts = (
        accounts.select("to_ID")
        .unique()
        .rename({"to_ID": "GL_Acc"})
        .join(account_descriptions, left_on="GL_Acc", right_on="%GL_ACC", how="left")
        .rename(
            {"GL Acc Desc": "GL_Acc_Desc", "GL Acc Type": "GL_Acc_Type", "GL_Acc": "ID"}
        )
    )

    return accounts
